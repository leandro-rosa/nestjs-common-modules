import { Global, Module } from '@nestjs/common'
import { S3Client, S3ClientConfig } from '@aws-sdk/client-s3'
import { ConfigService } from '@nestjs/config'
import { S3Service } from './s3'

const getConfig = (configService: ConfigService): S3ClientConfig => ({
  region: configService.get<string>('AWS_REGION')!,
  credentials: {
    accessKeyId: configService.get<string>('AWS_ACCESS_KEY_ID')!,
    secretAccessKey: configService.get<string>('AWS_SECRET_ACCESS_KEY')!,
  },
  endpoint: configService.get<string>('AWS_S3_ENDPOINT'),
  forcePathStyle: configService.get<boolean>('AWS_S3_FORCE_PATH_STYLE') ?? true,
});

const S3ServiceFactory = {
  provide: S3Service,
  useFactory: (configService: ConfigService) => {
    const s3 = new S3Client(getConfig(configService))
    return new S3Service(s3, configService)
  },
  inject: [ConfigService],
}

@Global()
@Module({
  providers: [S3ServiceFactory],
  exports: [S3ServiceFactory],
})
export class AwsModule {}
