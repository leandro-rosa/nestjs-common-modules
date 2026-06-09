import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs'
import * as path from 'path'
import { getDMMF } from '@prisma/sdk'

const entitiesOutputDir = path.resolve(process.cwd(), 'prisma', 'generated', 'entities')
const prismaSchemaPath = path.resolve(process.cwd(), 'prisma', 'schema.prisma')

if (!existsSync(entitiesOutputDir)) {
  mkdirSync(entitiesOutputDir, { recursive: true })
}

function toKebabCase(str: string): string {
  return str.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase()
}

function mapPrismaTypeToTS(prismaType: string, isList: boolean, kind: string): string {
  if (kind === 'object') {
    const baseType = `Partial<${prismaType}Entity>`
    return isList ? `${baseType}[]` : baseType
  }
  let tsType: string
  switch (prismaType) {
    case 'Int':
    case 'Float':
      tsType = 'number'
      break
    case 'Boolean':
      tsType = 'boolean'
      break
    case 'DateTime':
      tsType = 'Date'
      break
    case 'Json': // new case for JSON
      tsType = 'any'
      break
    default:
      tsType = 'string'
      break
  }
  return isList ? `${tsType}[]` : tsType
}

function generateEntityContent(model: any): string {
  const className = `${model.name}Entity`
  let content = `// filepath: /digital-orchestrator/digital-orchestrator/prisma/generated/entities/${toKebabCase(model.name)}.ts
/**
 * Auto-generated entity for ${model.name}.
 * This file is generated dynamically. DO NOT EDIT.
 */
import { ObjectType, Field, ID } from '@nestjs/graphql';
import { GraphQLJSONObject } from 'graphql-type-json';
`
  const dependencies = new Set<string>()
  for (const field of model.fields) {
    if (field.kind === 'object' && field.type !== model.name) {
      dependencies.add(field.type)
    }
  }
  dependencies.forEach(dep => {
    content += `import { ${dep}Entity } from './${toKebabCase(dep)}';\n`
  })

  content += `
@ObjectType({ description: 'Auto-generated ${model.name} entity' })
export class ${className} {
`
  const mapScalarToGraphQL = (type: string): string => {
    switch (type) {
      case 'Int':
      case 'Float':
        return 'Number'
      case 'Boolean':
        return 'Boolean'
      case 'DateTime':
        return 'Date'
      default:
        return 'String'
    }
  }

  for (const field of model.fields) {
    const optional = '?' // force optional for all fields
    const tsType = mapPrismaTypeToTS(field.type, field.isList, field.kind)
    let fieldDecorator = ''
    if (field.name === 'id') {
      fieldDecorator = `  @Field(() => ID, { nullable: true })\n`
    } else if (field.kind === 'scalar') {
      if (field.type === 'Json') {
        // use GraphQLJSONObject for JSON fields; mark as nullable
        fieldDecorator = field.isList
          ? `  @Field(() => [GraphQLJSONObject], { nullable: true })\n`
          : `  @Field(() => GraphQLJSONObject, { nullable: true })\n`
      } else {
        const gqlType = ((): string => {
          switch (field.type) {
            case 'Int':
            case 'Float':
              return 'Number'
            case 'Boolean':
              return 'Boolean'
            case 'DateTime':
              return 'Date'
            default:
              return 'String'
          }
        })()
        fieldDecorator = field.isList
          ? `  @Field(() => [${gqlType}], { nullable: true })\n`
          : `  @Field(() => ${gqlType}, { nullable: true })\n`
      }
    } else {
      // For relation fields, always mark as nullable.
      fieldDecorator = field.isList
        ? `  @Field(() => [${field.type}Entity], { nullable: true })\n`
        : `  @Field(() => ${field.type}Entity, { nullable: true })\n`
    }
    content += fieldDecorator + `  ${field.name}${optional}: ${tsType};\n\n`
  }
  content += `}\n`
  return content
}

async function generateEntities() {
  const prismaSchema = readFileSync(prismaSchemaPath, 'utf-8')
  const dmmf = await getDMMF({ datamodel: prismaSchema })
  for (const model of dmmf.datamodel.models) {
    const entityContent = generateEntityContent(model)
    const fileName = `${toKebabCase(model.name)}.ts`
    const filePath = path.join(entitiesOutputDir, fileName)
    writeFileSync(filePath, entityContent)
    console.log(`Entity generated: ${filePath}`)
  }
}

generateEntities().catch(err => {
  console.error(err)
  process.exit(1)
})
