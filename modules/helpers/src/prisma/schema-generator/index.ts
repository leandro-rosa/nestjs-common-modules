import { readFileSync, writeFileSync } from 'fs'
import * as path from 'path'
import { getDMMF } from '@prisma/sdk'
const outputPath = path.resolve(process.cwd(), 'prisma', 'generated', 'schema.gql')

function mapPrismaTypeToGraphQL(prismaType: string, isList: boolean, kind: string): string {
  if (kind === 'object') {
    return isList ? `[${prismaType}]` : prismaType
  }
  let gqlType
  switch (prismaType) {
    case 'Int':
      gqlType = 'Int'
      break
    case 'Float':
      gqlType = 'Float'
      break
    case 'Boolean':
      gqlType = 'Boolean'
      break
    case 'DateTime':
      gqlType = 'String'
      break
    default:
      gqlType = 'String'
  }
  return isList ? `[${gqlType}]` : gqlType
}

async function transformPrismaToGraphQL(): Promise<string> {
  const prismaSchemaPath = path.resolve(process.cwd(), 'prisma', 'schema.prisma')
  const prismaSchema = readFileSync(prismaSchemaPath, 'utf-8')
  const dmmf = await getDMMF({ datamodel: prismaSchema })
  let out = '# Generated GraphQL schema based on Prisma models\n\n'
  for (const model of dmmf.datamodel.models) {
    out += `type ${model.name} {\n`
    for (const field of model.fields) {
      out += `  ${field.name}: ${mapPrismaTypeToGraphQL(field.type, field.isList, field.kind)}\n`
    }
    out += `}\n\n`
  }
  return out
}

export async function generateGraphQLSchema() {
  const generatedSchema = await transformPrismaToGraphQL()
  writeFileSync(outputPath, generatedSchema)
  console.log('Schema generated at:', outputPath)
}

if (require.main === module) {
  generateGraphQLSchema().catch(err => {
    console.error(err)
    process.exit(1)
  })
}
