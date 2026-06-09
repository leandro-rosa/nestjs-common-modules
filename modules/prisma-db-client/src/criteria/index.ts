export type SearchCriteriaInclude<T, IncludeType = any> = {
  [P in keyof IncludeType]?: boolean | SearchCriteriaInclude<T, IncludeType[P]>
}

export interface WhereOperators {
  $lt?: any
  $lte?: any
  $gt?: any
  $gte?: any
  $not?: any
  $like?: string
  $ilike?: string
  $in?: any[]
  $or?: WhereCondition[]
  $notIn?: any[]
  $between?: any[]
  $eq?: any
  $ne?: any
  $is?: any
  $isNot?: any
  $contains?: any
  $startsWith?: any
  $endsWith?: any
}

export type WhereCondition = {
  [key: string]: any | WhereOperators
}

export interface SearchCriteriaInterface<T, IncludeType = any> {
  where?: Partial<T> | any
  cursor?: Partial<T> | any
  orderBy?: { [key in keyof T | string]?: 'asc' | 'desc' } | { [key: string]: 'asc' | 'desc' } | any
  by?: any
  take?: number
  skip?: number
  include?: SearchCriteriaInclude<T, IncludeType> | any
  select?: SearchCriteriaInclude<T, IncludeType>
  distinct?: string[]
}
