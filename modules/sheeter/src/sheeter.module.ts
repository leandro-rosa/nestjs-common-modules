import { Global, Module } from '@nestjs/common'
import { CsvWriterService } from './services/csv-writer';
import { SheeterProcessorService } from './services/processor';
import { XlsWriterService } from './services/xls-writer';

@Global()
@Module({
  providers: [SheeterProcessorService, XlsWriterService, CsvWriterService],
  exports: [SheeterProcessorService, XlsWriterService, CsvWriterService],
})
export class SheeterModule {}
