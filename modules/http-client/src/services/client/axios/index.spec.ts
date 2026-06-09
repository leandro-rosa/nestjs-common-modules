import { AxiosHttpClient } from '.'
import { HttpService } from '@nestjs/axios'
import { AxiosResponse } from 'axios'
import { RequestDTO, ResponseDTO } from './types'
import { of } from 'rxjs'
import { faker } from '@faker-js/faker'
import { Test } from '@nestjs/testing'

const mockHttpService = {
  request: jest.fn(),
}

const mockHttpAgentService = {
  getAgents: jest.fn().mockReturnValue({ httpsAgent: {}, httpAgent: {} }),
}

const mockIntegrationLogsRepository = {
  create: jest.fn().mockReturnValue({ httpsAgent: {}, httpAgent: {} }),
}
const mockDatadogAsyncLogManagerService = {
  createAsyncLog: jest.fn().mockReturnValue({ httpsAgent: {}, httpAgent: {} }),
}

describe.skip('AxiosHttpClient', () => {
  let httpClient: AxiosHttpClient

  beforeEach(async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [{ provide: HttpService, useValue: mockHttpService }, AxiosHttpClient],
    }).compile()

    httpClient = moduleRef.get<AxiosHttpClient>(AxiosHttpClient)
  })

  describe('send', () => {
    it('should send the request without headers and return the response', async () => {
      const request: RequestDTO = {
        url: 'https://example.com',
        payload: { key: 'value' },
        http_method: 'POST',
        entity_id: faker.number.int(),
        entity_type: faker.word.verb(),
      }

      const responseData = { result: 'success' }
      const responseStatus = 200
      const expectedResponse: ResponseDTO<any> = {
        response: { data: responseData, status: responseStatus },
        request: request.payload,
        uri: request.url,
      }

      const axiosResponse = {
        data: responseData,
        status: responseStatus,
        statusText: '',
        headers: {},
        config: {},
      } as AxiosResponse

      mockHttpService.request.mockReturnValue(of(axiosResponse))

      const result = await httpClient.send(request)

      expect(result).toEqual(expectedResponse)
      expect(mockDatadogAsyncLogManagerService.createAsyncLog).toBeCalledWith({
        data: {
          entity_type: request.entity_type,
          entity_id: request.entity_id,
          endpoint: request.url,
          request: JSON.stringify(request),
          response: JSON.stringify(result.response.data),
        },
        message: `Type: ${request.entity_type} Endpoint: ${request.url} - External Request`,
        log_key: 'external_request',
        level: 'info',
      })
    })

    it('should send the request and return the response', async () => {
      const request: RequestDTO = {
        url: 'https://example.com',
        payload: { key: 'value' },
        http_method: 'POST',
        headers: { Authorization: 'Bearer token' },
      }

      const responseData = { result: 'success' }
      const responseStatus = 200
      const expectedResponse: ResponseDTO<any> = {
        response: { data: responseData, status: responseStatus },
        request: request.payload,
        uri: request.url,
      }

      const axiosResponse = {
        data: responseData,
        status: responseStatus,
        statusText: '',
        headers: {},
        config: {},
      } as AxiosResponse

      mockHttpService.request.mockReturnValue(of(axiosResponse))

      const result = await httpClient.send(request)

      expect(result).toEqual(expectedResponse)
      expect(mockDatadogAsyncLogManagerService.createAsyncLog).toBeCalledWith({
        data: {
          entity_type: request.entity_type,
          entity_id: request.entity_id,
          endpoint: request.url,
          request: JSON.stringify(request),
          response: JSON.stringify(result.response.data),
        },
        message: `Type: ${request.entity_type} Endpoint: ${request.url} - External Request`,
        log_key: 'external_request',
        level: 'info',
      })
    })

    it('should handle error and return the response with error message', async () => {
      const request: RequestDTO = {
        url: 'https://example.com',
        payload: { key: 'value' },
        http_method: 'POST',
        headers: { Authorization: 'Bearer token' },
      }

      const errorMessage = 'Request failed'
      const expectedResponse: ResponseDTO<any> = {
        response: { error: errorMessage },
        request: request.payload,
        uri: request.url,
      }

      mockHttpService.request.mockImplementation(() => {
        throw new Error(errorMessage)
      })

      const result = await httpClient.send(request)

      expect(result).toEqual(expectedResponse)
      expect(mockDatadogAsyncLogManagerService.createAsyncLog).toBeCalledWith({
        data: {
          entity_type: request.entity_type,
          entity_id: request.entity_id,
          endpoint: request.url,
          message: `Error: ${errorMessage}`,
          request: JSON.stringify(request),
          response: JSON.stringify(result.response),
        },
        message: `External Request`,
        log_key: 'external_request',
        level: 'error',
      })
    })
  })
})
