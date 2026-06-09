export class SheeterProcessMessageDto<T = Record<string, any>, A = any> {
  rowData: T
  requestId: string
  rowId: number
  additionalData?: A
}
