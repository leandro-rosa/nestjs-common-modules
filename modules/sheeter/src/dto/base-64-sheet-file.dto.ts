import { IsString, IsNumber, IsNotEmpty, IsObject } from 'class-validator'

export class Base64SheetFileDto {
  @IsString()
  @IsNotEmpty()
  name: string

  @IsString()
  @IsNotEmpty()
  type: string

  @IsNumber()
  size: number

  @IsString()
  @IsNotEmpty()
  link: string
}

export class CreateBySheetsBase64Dto {
  [key: string]: Base64SheetFileDto
}
