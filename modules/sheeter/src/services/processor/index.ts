import os from 'os'
import { Injectable, Logger } from '@nestjs/common'
import { Readable as NodeReadable } from 'stream'
import { ReadableStream as WebReadableStream } from 'stream/web'
import * as ExcelJS from 'exceljs'
import slugify from 'slugify'
import { SheeterProcessMessageDto } from '@app/sheeter/dto/queue'
import { HoldItBullMQBroker } from '@app/hold-it/services/brokers/bull-mq'
import { promises as fsp } from 'fs'
import * as XLSX from 'xlsx'
import { Job } from 'bullmq'

import * as fs from 'fs'
import { Readable } from 'stream'

XLSX.set_fs(fs)
XLSX.stream.set_readable(Readable)

type RowsPerSheet = Array<{ sheet: string; rows: any[][] }>

@Injectable()
export class SheeterProcessorService {
  /**
   * Logger instance for this service.
   */
  private readonly logger = new Logger(SheeterProcessorService.name)

  /**
   * Chunk size used to batch queue messages.
   */
  private static readonly BATCH_SIZE = 1200

  /**
   * Default BullMQ job options to avoid Redis growth.
   */
  private static readonly DEFAULT_JOB_OPTIONS = { removeOnComplete: 12, removeOnFail: false }

  constructor(private readonly broker: HoldItBullMQBroker) {}

  /**
   * Process a spreadsheet (path or stream) into queue messages in chunks.
   * Supports dynamic header rows per sheet and falls back to SheetJS for non-xlsx/legacy files.
   */
  async smartChunk<T = Record<string, any>, A = any>({
    filePath,
    fileStream,
    queueCallbackName,
    requestId,
    additionalData,
    headersRow = [1],
  }: {
    filePath?: string
    fileStream?: NodeJS.ReadableStream | NodeReadable | WebReadableStream<any>
    requestId: string
    queueCallbackName: string
    additionalData?: A
    headersRow?: number[]
  }): Promise<{ jobs: Array<Job> }> {
    if (!filePath && !fileStream) {
      throw new Error('Either filePath or fileStream must be provided')
    }

    let rowsPerSheet: RowsPerSheet = []

    if (filePath) {
      await this.debugAssertFileReadable(filePath) // antes de ler
      await this.ensureReadableWithRetry(filePath)

      // Protect against temp/lock files like "~$file.xlsx"
      if (this.isTempExcelLockFile(filePath)) {
        throw new Error(`Temporary Excel lock file is not processable: ${filePath}`)
      }
      rowsPerSheet = await this.loadRowsFromPath(filePath)
    } else {
      rowsPerSheet = await this.loadRowsFromStream(fileStream!)
    }

    const jobs: Array<Job> = []
    for (const { sheet, rows } of rowsPerSheet) {
      let headers: string[] = []
      const pending: Array<SheeterProcessMessageDto<T>> = []

      for (let i = 0; i < rows.length; i++) {
        const rowNumber = i + 1
        const values = rows[i] ?? []

        // When current row is a header row for this sheet
        if (headersRow.includes(rowNumber)) {
          headers = this.normalizeHeaders(values)
          continue
        }

        // Build row object (values are 0-based arrays here)
        const rowData = this.buildRowObject(headers, values)

        // Skip fully empty rows
        if (this.isRowEmpty(rowData)) continue

        pending.push({
          rowData: rowData as T,
          requestId,
          rowId: rowNumber,
          additionalData: { ...(additionalData as object), worksheetName: sheet } as A & { worksheetName: string },
        })

        if (pending.length >= SheeterProcessorService.BATCH_SIZE) {
          const chunk = pending.splice(0, pending.length)
          const chunkJobs = await this.enqueueMany(queueCallbackName, chunk)
          jobs.push(...chunkJobs)
        }
      }

      if (pending.length > 0) {
        const leftoverJobs = await this.enqueueMany(queueCallbackName, pending)
        jobs.push(...leftoverJobs)
      }
    }

    return { jobs }
  }

  /**
   * Streamed (row-by-row) processing using ExcelJS streaming reader.
   * Use it when you want constant memory usage for giant files.
   */
  async processByChunk<T = Record<string, any>, A = any>(
    fileStream: NodeReadable | NodeJS.ReadableStream | WebReadableStream<any>,
    requestId: string,
    queueCallbackName: string,
    additionalData?: A,
  ): Promise<{ jobs: Array<Job> }> {
    const nodeStream = this.toNodeReadable(fileStream)
    const workbook = new (ExcelJS as any).stream.xlsx.WorkbookReader(nodeStream, {
      entries: 'emit',
      sharedStrings: 'cache',
      hyperlinks: 'cache',
      styles: 'cache',
      worksheets: 'emit',
    })

    const jobs: Array<Job> = []
    let headers: string[] = []
    const pending: Array<SheeterProcessMessageDto<T>> = []

    for await (const worksheet of workbook) {
      for await (const row of worksheet) {
        const values = Array.isArray(row.values) ? row.values.slice(1) : [] // row.values (1-based) → slice(1)

        if (!headers.length) {
          headers = this.normalizeHeaders(values)
          continue
        }

        const rowData = this.buildRowObject(headers, values)
        if (this.isRowEmpty(rowData)) continue

        pending.push({
          rowData: rowData as T,
          requestId,
          rowId: row.number,
          additionalData,
        })

        if (pending.length >= SheeterProcessorService.BATCH_SIZE) {
          const chunk = pending.splice(0, pending.length)
          const chunkJobs = await this.enqueueMany(queueCallbackName, chunk)
          jobs.push(...chunkJobs)
        }
      }
    }

    if (pending.length) {
      const leftoverJobs = await this.enqueueMany(queueCallbackName, pending)
      jobs.push(...leftoverJobs)
    }

    return { jobs }
  }

  // -------------------------------
  // Loading helpers
  // -------------------------------

  /**
   * Load rows from a path. Uses ExcelJS for valid .xlsx (zip) files; falls back to SheetJS otherwise.
   */
  private async loadRowsFromPath(filePath: string): Promise<RowsPerSheet> {
    const probe = await this.probeXlsx(filePath)
    this.logger.warn(`XLSX probe: ${JSON.stringify(probe)} for ${filePath}`)

    if (!probe.ok) {
      throw new Error(`XLSX probe failed: ${probe.reason}`)
    }

    // Tente ExcelJS primeiro SEMPRE; se falhar, o readWithExcelJSFile já faz fallback.
    return this.readWithExcelJSFile(filePath)
  }

  /**
   * Load rows from a stream. Try ExcelJS in-memory first, then fallback to SheetJS buffering.
   */
  private async loadRowsFromStream(
    fileStream: NodeJS.ReadableStream | NodeReadable | WebReadableStream<any>,
  ): Promise<RowsPerSheet> {
    // Try ExcelJS read(stream) first
    try {
      const nodeStream = this.toNodeReadable(fileStream)
      return await this.readWithExcelJSStream(nodeStream)
    } catch (err) {
      this.logger.warn(`ExcelJS stream read failed; falling back to SheetJS. Reason: ${(err as Error)?.message}`)
      // Fallback: buffer the stream and read with SheetJS
      const buffer = await this.bufferFromStream(fileStream)
      return this.readWithSheetJSBuffer(buffer)
    }
  }

  /**
   * Read path with ExcelJS and return rows per sheet (values normalized 0-based).
   */
  private async readWithExcelJSFile(filePath: string): Promise<RowsPerSheet> {
    try {
      const wb = new (ExcelJS as any).Workbook()
      await wb.xlsx.readFile(filePath)
      if (!Array.isArray(wb.worksheets) || wb.worksheets.length === 0) {
        this.logger.warn(`ExcelJS loaded but no worksheets: ${filePath} → fallback to SheetJS`)
        return this.readWithSheetJSFile(filePath)
      }
      return wb.worksheets.map((ws: any) => {
        const out: any[][] = []
        for (let r = 1; r <= ws.rowCount; r++) {
          const row = ws.getRow(r)
          out.push(Array.isArray(row.values) ? row.values.slice(1) : [])
        }
        return { sheet: ws.name, rows: out }
      })
    } catch (e: any) {
      this.logger.warn(`ExcelJS readFile failed: ${e?.message} → fallback to SheetJS (${filePath})`)
      return this.readWithSheetJSFile(filePath)
    }
  }

  /**
   * Read Node stream with ExcelJS and return rows per sheet (values normalized 0-based).
   */
  private async readWithExcelJSStream(nodeStream: NodeReadable): Promise<RowsPerSheet> {
    const workbook = new ExcelJS.Workbook()
    await workbook.xlsx.read(nodeStream)

    return workbook.worksheets.map(ws => {
      const rows: any[][] = []
      for (let r = 1; r <= ws.rowCount; r++) {
        const row = ws.getRow(r)
        const vals = Array.isArray(row.values) ? row.values.slice(1) : []
        rows.push(vals)
      }
      return { sheet: ws.name, rows }
    })
  }

  /**
   * Read path with SheetJS (handles CSV, legacy .xls (CFB), non-standard zips).
   */
  private readWithSheetJSFile(filePath: string): RowsPerSheet {
    try {
      const buf = fs.readFileSync(filePath) // se falhar aqui, é FS (ENOENT/EACCES)
      const wb = XLSX.read(buf, { type: 'buffer', cellDates: true })
      return this.rowsPerSheetFromSheetJS(wb)
    } catch (err: any) {
      this.logger.warn(`SheetJS buffer read failed: code=${err?.code} msg=${err?.message}. Trying XLSX.readFile...`)
      const wb = XLSX.readFile(filePath, { cellDates: true }) // último recurso
      return this.rowsPerSheetFromSheetJS(wb)
    }
  }

  /**
   * Read buffer with SheetJS (used as fallback after buffering a stream).
   */
  private readWithSheetJSBuffer(buffer: Buffer): RowsPerSheet {
    const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true })
    return this.rowsPerSheetFromSheetJS(wb)
  }

  /**
   * Convert a SheetJS workbook to RowsPerSheet structure.
   */
  private rowsPerSheetFromSheetJS(wb: XLSX.WorkBook): RowsPerSheet {
    return wb.SheetNames.map(name => ({
      sheet: name,
      rows: XLSX.utils.sheet_to_json<any[]>(wb.Sheets[name], {
        header: 1,
        blankrows: false,
        defval: null,
      }),
    }))
  }

  // -------------------------------
  // Queue helpers
  // -------------------------------

  /**
   * Enqueue a batch of messages to the given queue with standard job options.
   */
  private async enqueueMany<T>(
    queueCallbackName: string,
    messages: Array<SheeterProcessMessageDto<T>>,
  ): Promise<Array<Job>> {
    const payload = messages.map(m => ({
      ...m,
      jobOptions: SheeterProcessorService.DEFAULT_JOB_OPTIONS,
    }))
    return this.broker.holdItALot<SheeterProcessMessageDto<T>>({
      queueName: queueCallbackName,
      messages: payload,
    }) as any
  }

  // -------------------------------
  // Header / Row helpers
  // -------------------------------

  /**
   * Normalize and uniquify headers. Uses slugify and ensures non-empty unique names.
   */
  private normalizeHeaders(values: any[]): string[] {
    const raw = values.map((v, idx) => {
      const base =
        slugify(String(v ?? ''), {
          replacement: '_',
          lower: false,
          remove: /[*+~.()'"!:@/|[\]{}]/g,
        }) || `column_${idx + 1}`
      return base
    })

    const seen = new Map<string, number>()
    return raw.map(h => {
      const c = (seen.get(h) ?? 0) + 1
      seen.set(h, c)
      return c === 1 ? h : `${h}_${c}`
    })
  }

  /**
   * Build an object from headers + values (values 0-based).
   */
  private buildRowObject(headers: string[], values: any[]): Record<string, any> {
    return headers.reduce(
      (acc, h, idx) => {
        acc[h] = values[idx] ?? null
        return acc
      },
      {} as Record<string, any>,
    )
  }

  /**
   * Check if every value in the row object is empty/null.
   */
  private isRowEmpty(row: Record<string, any>): boolean {
    return Object.values(row).every(v => v === null || v === '')
  }

  // -------------------------------
  // Stream / File helpers
  // -------------------------------

  /**
   * Probe a file to understand if it is a valid xlsx (zip) or legacy .xls (CFB) or something else.
   */
  async probeXlsx(filePath: string) {
    if (!fs.existsSync(filePath)) {
      return { ok: false, reason: 'not_found' as const }
    }
    const stat = await fsp.stat(filePath)
    if (!stat.isFile()) return { ok: false, reason: 'not_a_file' as const }
    if (stat.size < 8) return { ok: false, reason: 'too_small' as const }

    const fd = await fsp.open(filePath, 'r')
    try {
      const { buffer } = await fd.read(Buffer.alloc(8), 0, 8, 0)
      const isZip = buffer[0] === 0x50 && buffer[1] === 0x4b // 'PK'
      const isOldXlsCFB = buffer[0] === 0xd0 && buffer[1] === 0xcf && buffer[2] === 0x11 && buffer[3] === 0xe0
      return { ok: true, isZip, isOldXlsCFB, size: stat.size }
    } finally {
      await fd.close()
    }
  }

  /**
   * Buffer any readable (Web or Node) into memory.
   */
  private async bufferFromStream(
    stream: NodeJS.ReadableStream | NodeReadable | WebReadableStream<any>,
  ): Promise<Buffer> {
    const nodeStream = this.toNodeReadable(stream)
    const chunks: Buffer[] = []
    await new Promise<void>((res, rej) => {
      nodeStream
        .on('data', c => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)))
        .once('end', () => res())
        .once('error', rej)
    })
    return Buffer.concat(chunks)
  }

  /**
   * Convert a Web ReadableStream to a Node Readable when necessary.
   * If it's already a Node stream, just normalize the type.
   */
  private toNodeReadable(s: NodeJS.ReadableStream | NodeReadable | WebReadableStream<any>): NodeReadable {
    const anyS = s as any
    if (typeof anyS.pipe === 'function') {
      return s as unknown as NodeReadable
    }
    // Node >= 18 has Readable.fromWeb
    return NodeReadable.fromWeb(s as unknown as WebReadableStream<any>)
  }

  /**
   * Detect temporary Excel lock files (e.g. "~$file.xlsx").
   */
  private isTempExcelLockFile(filePath: string): boolean {
    const base = filePath.split(/[\\/]/).pop() ?? filePath
    return base.startsWith('~$')
  }

  private async debugAssertFileReadable(p?: string) {
    const who = {
      pid: process.pid,
      uid: process.getuid?.(),
      gid: process.getgid?.(),
      cwd: process.cwd(),
      hostname: os.hostname(),
      envHost: process.env.HOSTNAME,
      node: process.version,
    }
    this.logger.warn(`PROC: ${JSON.stringify(who)}`)

    if (!p) return
    try {
      await fsp.access(p, fs.constants.R_OK)
      const st = await fsp.stat(p)
      this.logger.warn(`FS OK: ${p} (size=${st.size}, mode=${(st.mode & 0o777).toString(8)})`)
    } catch (e: any) {
      this.logger.error(`FS FAIL: ${p} code=${e?.code} msg=${e?.message}`)
    }
  }

  private async ensureReadableWithRetry(p: string, attempts = 3, delayMs = 250): Promise<void> {
    let lastErr: any
    for (let i = 0; i < attempts; i++) {
      try {
        await fsp.access(p, fs.constants.R_OK)
        const st = await fsp.stat(p)
        if (st.isFile() && st.size > 0) return
        lastErr = new Error(`not a non-empty file (size=${st.size})`)
      } catch (e) {
        lastErr = e
      }
      await new Promise(r => setTimeout(r, delayMs))
    }
    throw lastErr
  }
}
