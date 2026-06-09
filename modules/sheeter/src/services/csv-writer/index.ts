import { Injectable } from '@nestjs/common'
import * as fs from 'fs'
import * as path from 'path'
import { createObjectCsvWriter } from 'csv-writer'
import * as readline from 'readline'
import { ConfigService } from '@nestjs/config'
import { SearchCriteriaInterface } from '@leandro-rosa/prisma-db-client'
import { S3Service } from '@leandro-rosa/aws'

@Injectable()
export class CsvWriterService {
  constructor(
    private readonly s3Service: S3Service,
    private readonly configService: ConfigService,
  ) {}

  async appendRowData({ data, filePath }: { filePath: string; data: Record<string, any> }): Promise<string> {
    try {
      const isNewFile = !fs.existsSync(filePath)

      // Configuração do CSV Writer
      const csvWriter = createObjectCsvWriter({
        path: filePath,
        header: Object.keys(data).map(key => ({ id: key, title: key })),
        append: !isNewFile,
      })

      await csvWriter.writeRecords([data])
      return `Registro adicionado ao arquivo CSV: ${filePath}`
    } catch (error) {
      throw new Error(`Erro ao adicionar o registro: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  async appendRowsData({
    items,
    filePath,
    headers,
  }: {
    filePath: string
    items: Record<string, any>[]
    headers?: { id: string; title: string }[]
  }): Promise<string> {
    try {
      const firstItem = items[0]
      if (!firstItem) {
        return `Registros adicionados ao arquivo CSV: ${filePath}`
      }

      const isNewFile = !fs.existsSync(filePath)

      // Configuração do CSV Writer
      const csvWriter = createObjectCsvWriter({
        path: filePath,
        header: headers || Object.keys(firstItem).map(key => ({ id: key, title: key })),
        append: !isNewFile,
      })

      await csvWriter.writeRecords(items)
      return `Registros adicionados ao arquivo CSV: ${filePath}`
    } catch (error) {
      throw new Error(`Erro ao adicionar os registros: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  async createCsvByFunctionCallback<T extends object>({
    filePath,
    getItems,
    criteria,
    page = 0,
    pageSize = this.configService.get<number>('DEFAULT_PAGE_SIZE', 12),
  }: {
    filePath: string
    getItems: (page: number, pageSize: number, criteria?: SearchCriteriaInterface<T>) => Promise<T[]>
    criteria?: SearchCriteriaInterface<T>
    page?: number
    pageSize?: number
  }): Promise<string> {
    try {
      const isNewFile = !fs.existsSync(filePath)
      const headersSet = new Set<string>()
      const writeStream = fs.createWriteStream(filePath, { flags: isNewFile ? 'w' : 'a' })

      let hasMoreData = true

      while (hasMoreData) {
        const items = await getItems(page, pageSize, criteria)

        if (items.length === 0) {
          hasMoreData = false
          break
        }

        if (page === 0 && isNewFile) {
          const firstItem = items[0]
          if (!firstItem) {
            break
          }

          // Escrever cabeçalho
          const headers = Object.keys(firstItem).join(',')
          writeStream.write(headers + '\n')
          items.forEach(item => Object.keys(item).forEach(key => headersSet.add(key)))
        }

        // Escrever linhas
        for (const item of items) {
          const row = Object.values(item).join(',')
          writeStream.write(row + '\n')
        }

        page += 1
      }

      writeStream.end()
      return `Arquivo CSV criado com sucesso: ${filePath}`
    } catch (error) {
      throw new Error(`Erro ao criar o arquivo CSV: ${(error as Error).message}`)
    }
  }

  async uploadToS3({ filePath, s3Key }: { filePath: string; s3Key: string }): Promise<string> {
    try {
      const fileStream = fs.createReadStream(filePath, {
        encoding: 'utf-8',
        highWaterMark: 256 * 1024,
        autoClose: true,
      })
      await this.s3Service.uploadFile(s3Key, fileStream, 'text/csv')
      return `Arquivo CSV enviado para o S3: ${s3Key}`
    } catch (error) {
      throw new Error(`Erro ao enviar o arquivo para o S3: ${error instanceof Error ? error.message : String(error)}`)
    }
  }
}
