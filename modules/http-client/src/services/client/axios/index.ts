import { HttpException, Injectable, Logger } from '@nestjs/common'
import { AxiosRequestConfig } from 'axios'
import { HttpService } from '@nestjs/axios'
import { lastValueFrom } from 'rxjs'
import { RequestDTO, ResponseDTO } from './types'
import { setTimeout } from 'timers/promises'

@Injectable()
export class AxiosHttpClient {
  private readonly defaultHeaders = { 'Content-Type': 'application/json', 'User-Agent': 'lrosa/comolatti' }
  private readonly logger = new Logger(AxiosHttpClient.name)
  private readonly maxRetries = 12

  constructor(private readonly httpService: HttpService) {}

  async send<T = any, P = any>(request: RequestDTO<P>): Promise<ResponseDTO<Partial<T>>> {
    let attempt = 0
    let lastError: any = null

    while (attempt < this.maxRetries) {
      try {
        const config = this.buildRequest(request)
        const { data, status } = await lastValueFrom(this.httpService.request(config))

        // this.logger.debug({ request, data, status }, 'HTTP_REQUEST_SUCCESS')
        return {
          response: { data, status },
          request: request.payload,
          uri: request.url,
        }
      } catch (error: any) {
        lastError = error

        this.logger.error(
          {
            status: error?.response?.status,
            data: error?.response?.data,
            headers: error?.response?.headers,
          },
          error?.stack,
          'HTTP_REQUEST_ERROR_RESPONSE',
        )
        const isRetryable = error?.response?.status === 429 || error?.code === 'ECONNABORTED'

        attempt++
        if (attempt < this.maxRetries && isRetryable) {
          // Exponential backoff until maxRetries is reached. Base delay is 300ms, max 9.6s.
          const delay = 300 * Math.pow(2, attempt - 1)
          this.logger.warn({ request, error: lastError, attempt, delay }, 'HTTP_REQUEST_RETRYING')
          await setTimeout(delay)
        } else {
          break
        }
      }
    }
    if (request.throw_on_exception && lastError?.message) {
      this.logger.error({ request, error: lastError }, lastError.stack, 'HTTP_REQUEST_FAILED')
      throw new Error(lastError?.message)
    }

    const result = {
      response: { error: lastError?.message },
      request: request.payload,
      uri: request.url,
    }

    this.logger.error(`Error on request ${request?.http_method?.toUpperCase()}: ${request?.url}`, lastError?.stack, {
      entity_type: request.entity_type,
      entity_id: request.entity_id,
      endpoint: request.url,
      error_message: lastError?.message,
      request: JSON.stringify(request).slice(0, 1000),
      response: JSON.stringify(result.response),
    })

    if (lastError.message) {
      this.logger.error({ request, error: lastError }, lastError.stack, 'HTTP_REQUEST_FAILED')
      throw lastError
    }

    throw new HttpException(
      `Failed to send request after ${this.maxRetries} attempts: ${lastError?.message || 'Unknown error'}`,
      lastError?.response?.status || 500,
    )
  }

  // private buildRequest({ url, payload, http_method, headers: request_headers }: RequestDTO): AxiosRequestConfig {
  //   return {
  //     url,
  //     method: http_method,
  //     headers: { ...this.defaultHeaders, ...(request_headers ?? {}) },
  //     data: payload,
  //   }
  // }

  private buildRequest({
    url,
    payload,
    http_method,
    headers: requestHeaders,
    timeout,
    responseType,
    maxBodyLength,
    maxContentLength,
  }: RequestDTO): AxiosRequestConfig {
    return {
      url,
      method: http_method,
      headers: {
        ...this.defaultHeaders,
        ...(requestHeaders ?? {}),
      },
      data: payload,
      timeout,
      responseType,
      maxBodyLength: maxBodyLength ?? Infinity,
      maxContentLength: maxContentLength ?? Infinity,
      validateStatus: status => status >= 200 && status < 300,
    }
  }
}
