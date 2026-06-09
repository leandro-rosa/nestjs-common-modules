import { AxiosRequestConfig } from 'axios'

export interface RequestDTO<P = any> {
  http_method: AxiosRequestConfig['method']
  payload?: P
  headers?: Record<string, string>
  timeout?: number
  responseType?: AxiosRequestConfig['responseType']
  maxBodyLength?: number
  maxContentLength?: number
  throw_on_exception?: boolean
  entity_type?: string
  entity_id?: string | number
  url: string
}

export interface ResponseDTO<T = any> {
  response: { data?: T; status?: any | number; error?: any }

  request?: any

  uri: string
}
