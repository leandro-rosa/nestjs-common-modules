import { Injectable } from '@nestjs/common'
import {
  DeleteObjectCommand,
  DeleteObjectCommandOutput,
  GetObjectCommand,
  GetObjectCommandOutput,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
  PutObjectCommand,
  PutObjectCommandOutput,
  S3Client,
  S3ClientConfig,
} from '@aws-sdk/client-s3'

import { ConfigService } from '@nestjs/config'
import { Readable } from 'stream'

export interface S3CustomConfig {
  region?: string
  accessKeyId?: string
  secretAccessKey?: string
  endpoint?: string
  forcePathStyle?: boolean
}

@Injectable()
export class S3Service {
  constructor(
    private readonly defaultS3Client: S3Client,
    private readonly configService: ConfigService,
  ) {}

  private getClient(config?: S3CustomConfig): S3Client {
    if (!config) {
      return this.defaultS3Client
    }

    const s3Config: S3ClientConfig = {
      region: config.region ?? this.configService.get<string>('AWS_REGION')!,
      credentials: {
        accessKeyId: config.accessKeyId ?? this.configService.get<string>('AWS_ACCESS_KEY_ID')!,
        secretAccessKey: config.secretAccessKey ?? this.configService.get<string>('AWS_SECRET_ACCESS_KEY')!,
      },
      endpoint: config.endpoint ?? this.configService.get<string>('AWS_S3_ENDPOINT'),
      forcePathStyle: config.forcePathStyle ?? this.configService.get<boolean>('AWS_S3_FORCE_PATH_STYLE') ?? true,
    }

    return new S3Client(s3Config)
  }

  async uploadFile(
    key: string,
    body: Buffer | Readable,
    contentType: string,
    options?: {
      bucket?: string
      connection?: S3CustomConfig
    },
  ): Promise<PutObjectCommandOutput> {
    const client = this.getClient(options?.connection)

    const command = new PutObjectCommand({
      Bucket: options?.bucket ?? this.configService.get<string>('AWS_S3_BUCKET') ?? '',
      Key: key,
      Body: body,
      ContentType: contentType,
    })

    return client.send(command)
  }
}
