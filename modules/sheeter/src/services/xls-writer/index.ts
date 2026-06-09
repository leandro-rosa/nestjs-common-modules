import { Injectable } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import * as ExcelJS from 'exceljs'
import * as path from 'path'
import * as fs from 'fs'
import { S3Service } from '@leandro-rosa/aws'
import { HoldItBullMQBroker } from '@leandro-rosa/hold-it'
import { SearchCriteriaInterface } from '@leandro-rosa/prisma-db-client'

@Injectable()
export class XlsWriterService {
  constructor(
    private readonly holdItBroker: HoldItBullMQBroker,
    private readonly s3Service: S3Service,
    private readonly configService: ConfigService,
  ) {}

  createWorkbook() {
    return new ExcelJS.Workbook()
  }

  async appendRowData({
    data,
    filePath,
    worksheetName,
  }: {
    filePath: string
    worksheetName: string
    data: Record<string, any>
  }): Promise<string> {
    try {
      const isNewFile = !fs.existsSync(filePath)

      if (isNewFile) {
        const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: filePath })
        const worksheet = workbook.addWorksheet(worksheetName)

        worksheet.columns = Object.keys(data).map(key => ({ header: key, key }))
        worksheet.addRow(data).commit()

        await workbook.commit()
        return `Novo arquivo criado e registro adicionado: ${filePath}`
      }

      const workbook = this.createWorkbook()
      await workbook.xlsx.readFile(filePath)

      let worksheet = workbook.getWorksheet(worksheetName)
      if (!worksheet) {
        worksheet = workbook.addWorksheet(worksheetName)
        worksheet.columns = Object.keys(data).map(key => ({ header: key, key }))
      }

      worksheet.addRow(data)

      await workbook.xlsx.writeFile(filePath)
      return `Registro adicionado ao arquivo existente: ${filePath}`
    } catch (error) {
      throw new Error(`Erro ao adicionar o registro: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  async appendRowsData({
    items,
    filePath,
    worksheetName,
  }: {
    filePath: string
    worksheetName: string
    items: Record<string, any>[]
  }): Promise<string> {
    try {
      const firstItem = items[0]
      if (!firstItem) {
        return `Novo arquivo criado e registro adicionado: ${filePath}`
      }

      const isNewFile = !fs.existsSync(filePath)

      if (isNewFile) {
        const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: filePath })
        const worksheet = workbook.addWorksheet(worksheetName)
        worksheet.columns = Object.keys(firstItem).map(key => ({ header: key, key }))
        await Promise.all(items.map(item => worksheet.addRow(item).commit()))

        await workbook.commit()
        return `Novo arquivo criado e registro adicionado: ${filePath}`
      }

      const workbook = this.createWorkbook()
      await workbook.xlsx.readFile(filePath)

      let worksheet = workbook.getWorksheet(worksheetName)
      if (!worksheet) {
        worksheet = workbook.addWorksheet(worksheetName)
      }

      worksheet.columns = Object.keys(firstItem).map(key => ({ header: key, key }))

      await Promise.all(items.map(item => worksheet.addRow(item)))

      await workbook.xlsx.writeFile(filePath)
      return `Novo arquivo criado e registro adicionado: ${filePath}`
    } catch (error) {
      throw new Error(`Erro ao adicionar o registro: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  async createWorkbookByFunctionCallback<T extends object>({
    workbook,
    worksheetData,
    workbookFileName,
    criteria,
    page = 0,
    pageSize = this.configService.get<number>('DEFAULT_PAGE_SIZE', 12),
  }: {
    workbook?: ExcelJS.Workbook
    worksheetData: {
      worksheetName: string
      getItems: (page: number, pageSize: number, criteria?: SearchCriteriaInterface<T>) => Promise<T[]>
    }
    workbookFileName: string
    criteria?: SearchCriteriaInterface<T>
    page?: number
    pageSize?: number
  }): Promise<ExcelJS.Workbook> {
    const wb = workbook ?? new ExcelJS.Workbook()
    const worksheet = wb.addWorksheet(worksheetData.worksheetName)
    let hasMoreData = true

    while (hasMoreData) {
      const items = await worksheetData.getItems(page, pageSize, criteria)

      if (items.length === 0) {
        hasMoreData = false
        break
      }

      if (page === 0) {
        const firstItem = items[0]
        if (!firstItem) {
          break
        }

        worksheet.columns = Object.keys(firstItem).map(key => ({ header: key, key }))
      }

      items.forEach(item => {
        worksheet.addRow(item)
      })

      page += 1
    }

    const tempFilePath = path.join(__dirname, `${workbookFileName}.xlsx`)
    await wb.xlsx.writeFile(tempFilePath)

    await this.s3Service.uploadFile(
      `${workbookFileName}.xlsx`,
      fs.createReadStream(tempFilePath, { encoding: 'utf-8', highWaterMark: 256 * 1024 }),
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )

    fs.unlinkSync(tempFilePath)

    return wb
  }

  async createWorksheetByEntity<T extends object>({
    entity,
    workbook,
    workbookFileName,
  }: {
    entity: { entityName: string; items: T[] }
    workbookFileName: string
    workbook: ExcelJS.Workbook
  }) {
    let worksheet = workbook.getWorksheet(entity.entityName)
    if (!worksheet) {
      const firstItem = entity.items[0]
      if (!firstItem) {
        return workbook
      }

      worksheet = workbook.addWorksheet(entity.entityName)
      worksheet.columns = Object.keys(firstItem).map(key => ({ header: key, key }))
    }

    for (const row of entity.items) {
      worksheet.addRow(row)
    }

    return workbook
  }
}
