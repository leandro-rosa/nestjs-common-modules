import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs'
import * as path from 'path'
import { getDMMF } from '@prisma/sdk'

function kebabCase(str: string): string {
  return str.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase()
}

function snakeCase(str: string): string {
  return str.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase()
}

function pluralize(str: string): string {
  if (str.endsWith('y') && !['a', 'e', 'i', 'o', 'u'].includes(str.charAt(str.length - 2))) {
    return str.slice(0, -1) + 'ies'
  }
  return str + 's'
}

// New helper: converts string to camelCase (only lowercases first letter)
function camelCase(str: string): string {
  return str.charAt(0).toLowerCase() + str.slice(1)
}

const resolversOutputDir = path.resolve(process.cwd(), 'prisma', 'generated', 'resolvers')
const prismaSchemaPath = path.resolve(process.cwd(), 'prisma', 'schema.prisma')

if (!existsSync(resolversOutputDir)) {
  mkdirSync(resolversOutputDir, { recursive: true })
}

// Update extraDepthModels to include ProductCategory instead of ProductCategoryEntity.
const extraDepthModels = new Set<string>([
  'ProductAttribute',
  'ProductAttributeValue',
  'ProductSimilarity',
  'ProductInventory',
  'ProductCategory',
  'Category',
  'ProductAttributeSourceMapped',
  'ProductAttributeMapped',
])
function generateResolverContent(model: any, modelMap: Map<string, any>): string {
  const modelName = model.name
  // Dynamically build include clause with maxDepth = 1.
  const includeClause = (() => {
    const maxDepth = 1
    const buildIncludeClause = (m: any, map: Map<string, any>, ancestors = new Set<string>(), depth = 0): string => {
      const effectiveMaxDepth = extraDepthModels.has(m.name) ? maxDepth + 1 : maxDepth
      if (depth >= effectiveMaxDepth) return ''
      ancestors.add(m.name)
      let entries: string[] = []
      m.fields.forEach((f: any) => {
        if (f.kind === 'object' && f.relationName) {
          // Force include mapped_attributes for ProductAttribute model.
          if (m.name === 'ProductAttribute' && f.name === 'mapped_attributes') {
            entries.push(`${f.name}: true`)
          } else if (ancestors.has(f.type)) {
            entries.push(`${f.name}: true`)
          } else {
            const relatedModel = map.get(f.type)
            if (relatedModel) {
              const nested = buildIncludeClause(relatedModel, map, new Set(ancestors), depth + 1)
              if (nested) entries.push(`${f.name}: { include: ${nested} }`)
              else entries.push(`${f.name}: true`)
            } else {
              entries.push(`${f.name}: true`)
            }
          }
        }
      })
      return entries.length > 0 ? `{ ${entries.join(', ')} }` : ''
    }
    const clause = buildIncludeClause(model, modelMap)
    return clause ? `, include: ${clause}` : ''
  })()

  // Aggregate conversion rules per relation field.
  let fieldConversionMapping: { [key: string]: string } = {}
  let fieldConversionBlockSingle: { [key: string]: string } = {}
  const relationFields = model.fields.filter((f: any) => f.kind === 'object' && f.relationName)
  relationFields.forEach((f: any) => {
    const relatedModel = modelMap.get(f.type)
    if (relatedModel) {
      let conversions: string[] = []
      let singleConversions: string[] = []
      relatedModel.fields.forEach((rf: any) => {
        if (rf.type === 'Json') {
          if (f.isList) {
            if (rf.isList) {
              conversions.push(
                `${rf.name}: Array.isArray(subItem.${rf.name}) ? subItem.${rf.name} : (subItem.${rf.name} != null ? [subItem.${rf.name}] : [])`,
              )
              singleConversions.push(
                `${rf.name}: Array.isArray(subItem.${rf.name}) ? subItem.${rf.name} : (subItem.${rf.name} != null ? [subItem.${rf.name}] : [])`,
              )
            } else {
              conversions.push(`${rf.name}: subItem.${rf.name}`)
              singleConversions.push(`${rf.name}: subItem.${rf.name}`)
            }
          } else {
            if (rf.isList) {
              conversions.push(
                `${rf.name}: Array.isArray(item.${f.name}.${rf.name}) ? item.${f.name}.${rf.name} : (item.${f.name}.${rf.name} != null ? [item.${f.name}.${rf.name}] : [])`,
              )
              singleConversions.push(
                `${rf.name}: Array.isArray(result.${f.name}.${rf.name}) ? result.${f.name}.${rf.name} : (result.${f.name}.${rf.name} != null ? [result.${f.name}.${rf.name}] : [])`,
              )
            } else {
              conversions.push(`${rf.name}: item.${f.name}.${rf.name}`)
              singleConversions.push(`${rf.name}: result.${f.name}.${rf.name}`)
            }
          }
        }
      })
      if (conversions.length > 0) {
        if (f.isList) {
          fieldConversionMapping[f.name] =
            `Array.isArray(item.${f.name}) ? item.${f.name}.map(subItem => ({ ...subItem, ${conversions.join(', ')} })) : item.${f.name}`
          fieldConversionBlockSingle[f.name] =
            `if(Array.isArray(result.${f.name})) { result.${f.name} = result.${f.name}.map(subItem => ({ ...subItem, ${singleConversions.join(', ')} })) }`
        } else {
          fieldConversionMapping[f.name] =
            `item.${f.name} ? { ...item.${f.name}, ${conversions.join(', ')} } : item.${f.name}`
          fieldConversionBlockSingle[f.name] =
            `if(result.${f.name}) { result.${f.name} = { ...result.${f.name}, ${singleConversions.join(', ')} } }`
        }
      }
    }
  })

  // Generic conversion for self-relations: for any relation field that is a list, default to an empty array if null.
  model.fields
    .filter((f: any) => f.kind === 'object' && f.relationName && f.isList)
    .forEach((f: any) => {
      fieldConversionMapping[f.name] = `item.${f.name} || []`
      fieldConversionBlockSingle[f.name] = `result.${f.name} = result.${f.name} || []`
    })

  // Specific conversion for ProductAttribute.mapped_attributes: force empty array if null.
  if (modelName === 'ProductAttribute') {
    fieldConversionMapping['mapped_attributes'] = 'item.mapped_attributes !== null ? item.mapped_attributes : []'
    fieldConversionBlockSingle['mapped_attributes'] =
      'if(result.mapped_attributes === null) { result.mapped_attributes = [] }'
  }

  const finalConversionMapping = Object.keys(fieldConversionMapping)
    .map(key => `${key}: ${fieldConversionMapping[key]}`)
    .join(',\n')
  const finalConversionBlockSingle = Object.values(fieldConversionBlockSingle).join('\n')

  return `import 'reflect-metadata'
import { Resolver, Query, Mutation, Args } from '@nestjs/graphql';
import { GraphQLJSONObject } from 'graphql-type-json';
import { PrismaClientService } from '@l-rosa/prisma-db-client';
import { ${modelName}Entity } from '../entities/${kebabCase(modelName)}';

/**
 * Resolver for ${modelName} entity.
 *
 * This file is generated dynamically. DO NOT EDIT.
 */
@Resolver(() => ${modelName}Entity)
export class ${modelName}Resolver {
  constructor(private readonly prismaService: PrismaClientService) {}

  @Query(() => [${modelName}Entity], { name: '${pluralize(snakeCase(modelName))}' })
  async get${modelName}s(
    @Args('where', { type: () => GraphQLJSONObject, nullable: true }) where?: any,
    @Args('orderBy', { type: () => GraphQLJSONObject, nullable: true }) orderBy?: any,
    @Args('skip', { nullable: true }) skip?: number,
    @Args('take', { nullable: true }) take?: number,
  ): Promise<${modelName}Entity[]> {
    const result = await this.prismaService.${camelCase(modelName)}.findMany({ where, orderBy, skip, take${includeClause} });
    return (result.map(item => ({
      ...(item as any),
      ${finalConversionMapping}
    })) as unknown as ${modelName}Entity[]);
  }

  @Query(() => ${modelName}Entity, { name: '${snakeCase(modelName)}' })
  async get${modelName}(@Args('id') id: number): Promise<${modelName}Entity | null> {
    const result = await this.prismaService.${camelCase(modelName)}.findUnique({ where: { id }${includeClause} });
    ${finalConversionBlockSingle}
    return result as unknown as ${modelName}Entity | null;
  }

  @Mutation(() => ${modelName}Entity)
  async create${modelName}(@Args('data', { type: () => GraphQLJSONObject }) data: any): Promise<${modelName}Entity> {
    const result = await this.prismaService.${camelCase(modelName)}.create({ data${includeClause} });
    ${finalConversionBlockSingle}
    return result as unknown as ${modelName}Entity;
  }
}`
}

async function generateResolvers() {
  const prismaSchema = readFileSync(prismaSchemaPath, 'utf-8')
  const dmmf = await getDMMF({ datamodel: prismaSchema })
  // Build a map for easy lookup of model definitions by name
  const modelMap = new Map(dmmf.datamodel.models.map((m: any) => [m.name, m]))
  let indexExports = ''
  for (const model of dmmf.datamodel.models) {
    const resolverContent = generateResolverContent(model, modelMap)
    const fileName = `${kebabCase(model.name)}.ts`
    const filePath = path.join(resolversOutputDir, fileName)
    writeFileSync(filePath, resolverContent)
    indexExports += `export * from './${fileName.replace('.ts', '')}'\n`
  }
  const indexPath = path.join(resolversOutputDir, 'index.ts')
  const indexContent = `// filepath: ${indexPath}\n` + indexExports
  writeFileSync(indexPath, indexContent)
  console.log('Resolvers generated at:', resolversOutputDir)
}

generateResolvers().catch(err => {
  console.error(err)
  process.exit(1)
})
