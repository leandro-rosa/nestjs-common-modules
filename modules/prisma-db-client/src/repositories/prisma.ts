import { SearchCriteriaInterface } from '../criteria'
import { PrismaClientService } from '../services/client'

/**
 * Abstract repository for handling database operations with Prisma.
 * Includes support for transactions and error handling.
 */
export abstract class PrismaRepository<T = unknown, Model = unknown> {
  protected model: any
  protected entityName: string

  /**
   * Constructor for initializing the repository with the Prisma client service.
   * @param prisma - Instance of PrismaClientService.
   * @param model - Prisma model delegate.
   */
  protected constructor(
    protected readonly prisma: PrismaClientService,
    model: any,
    entityName: string,
  ) {
    this.model = model
    this.entityName = entityName

    // prisma.product.findMany()
  }

  /**
   * Removes audit fields (e.g., `created_at`, `updated_at`, `deleted_at`) from objects.
   * @param item - The object to clean.
   * @returns The cleaned object.
   */
  protected cleanAuditFields(item: Partial<Model>): Partial<Model> {
    if ('created_at' in item) {
      delete item.created_at
    }
    if ('updated_at' in item) {
      delete item.updated_at
    }
    if ('deleted_at' in item) {
      delete item.deleted_at
    }
    return item
  }

  protected async withErrorHandling(method: string, fn: () => Promise<any>, params?: any): Promise<any> {
    try {
      return await fn()
    } catch (error) {
      this.handleError(method, error, params)
    }
  }

  protected handleError(method: string, error: any, params?: any) {
    const paramsString = params
      ? JSON.stringify(params, (_, value) => (typeof value === 'bigint' ? value.toString() : value))
      : 'No parameters'
    const modelName = this.constructor.name

    console.error(
      `Error in ${method} method for model ${modelName}: ${error.message || error}. Params: ${paramsString}.`,
      error,
    )

    throw new Error(
      `Error in ${method} method for model ${modelName}: ${error.message || error}. Params: ${paramsString}.`,
    )
  }

  async findAll(criteria?: SearchCriteriaInterface<Model>): Promise<Partial<Model>[]> {
    return this.withErrorHandling(
      'findAll',
      async () => {
        const query = {
          ...(criteria?.where != null ? { where: criteria.where } : {}),
          ...(criteria?.orderBy != null ? { orderBy: criteria.orderBy } : {}),
          ...(criteria?.take != null ? { take: criteria.take } : {}),
          ...(criteria?.skip != null ? { skip: criteria.skip } : {}),
          ...(criteria?.include != null ? { include: criteria.include } : {}),
          ...(criteria?.select != null ? { select: criteria.select } : {}),
          ...(criteria?.distinct != null ? { distinct: criteria.distinct } : {}),
          ...(criteria?.cursor != null ? { cursor: criteria.cursor } : {}),
        }

        const items: Partial<Model>[] = (await this.model.findMany(query)) || []

        return items.map((item: Partial<Model>) => this.cleanAuditFields(item))
      },
      criteria,
    )
  }

  async count(criteria?: SearchCriteriaInterface<T>): Promise<number> {
    return this.withErrorHandling(
      'count',
      async () => {
        const query = {
          ...(criteria?.where != null ? { where: criteria.where } : {}),
          ...(criteria?.orderBy != null ? { orderBy: criteria.orderBy } : {}),
          ...(criteria?.take != null ? { take: criteria.take } : {}),
          ...(criteria?.skip != null ? { skip: criteria.skip } : {}),
          ...(criteria?.include != null ? { include: criteria.include } : {}),
          ...(criteria?.select != null ? { select: criteria.select } : {}),
          ...(criteria?.distinct != null ? { distinct: criteria.distinct } : {}),
          ...(criteria?.cursor != null ? { cursor: criteria.cursor } : {}),
        }

        return this.model.count(query)
      },
      criteria,
    )
  }

  async findUnique(criteria?: Partial<SearchCriteriaInterface<T>>): Promise<Partial<Model> | null> {
    return this.withErrorHandling(
      'findUnique',
      async () => {
        if (!criteria) throw new Error('Criteria is required.')

        const item = await this.model.findUnique(criteria)
        return item ? this.cleanAuditFields(item) : null
      },
      criteria,
    )
  }

  async create(data: Partial<Model>): Promise<Partial<Model>> {
    return this.withErrorHandling(
      'create',
      async () => {
        const item = await this.model.create({ data })
        return item ? this.cleanAuditFields(item) : null
      },
      data,
    )
  }

  async createMany(data: Partial<Model>[], skipDuplicates = true): Promise<{ count: number } | null> {
    return this.withErrorHandling(
      'createMany',
      async () => {
        return this.model.createMany({ data, skipDuplicates })
      },
      data,
    )
  }

  async updateByCriteria(
    data: Partial<Model>,
    criteria: Partial<SearchCriteriaInterface<T>>,
  ): Promise<{ count: number } | null> {
    return this.withErrorHandling(
      'updateByCriteria',
      async () => {
        const result = await this.model.updateMany({
          ...criteria,
          data,
        })

        return result
      },
      { data, criteria },
    )
  }

  async updateMany(
    data: Partial<Model>,
    criteria: Partial<SearchCriteriaInterface<T>>,
  ): Promise<{ count: number } | null> {
    return this.withErrorHandling(
      'updateMany',
      async () => {
        return this.model.updateMany({ ...criteria, data })
      },
      { data, criteria },
    )
  }

  async update(id: number | string, data: Partial<Model>): Promise<Partial<Model> | null> {
    return this.withErrorHandling(
      'update',
      async () => {
        const item = await this.model.update({ where: { id }, data })

        if (!item) return null
        delete item.created_at
        delete item.updated_at
        delete item.deleted_at
        return item
      },
      { id, data },
    )
  }

  async createUpdate(criteria: SearchCriteriaInterface<any>, data: Partial<Model>): Promise<Partial<Model> | null> {
    return this.withErrorHandling(
      'createUpdate',
      async () => {
        const item = await this.model.upsert({
          ...criteria,
          create: data,
          update: data,
        })

        if (!item) return null
        delete item.created_at
        delete item.updated_at
        delete item.deleted_at
        return item
      },
      { criteria, data },
    )
  }

  async delete(id: number | string): Promise<boolean> {
    return this.withErrorHandling(
      'delete',
      async () => {
        const result = await this.model.delete({ where: { id } })
        return Boolean(result)
      },
      { id },
    )
  }

  async deleteMany(criteria: SearchCriteriaInterface<any>): Promise<boolean> {
    return this.withErrorHandling(
      'deleteMany',
      async () => {
        const result = await this.model.deleteMany(criteria)
        return Boolean(result)
      },
      criteria,
    )
  }

  async findFirst(criteria: SearchCriteriaInterface<Model>): Promise<Partial<Model> | null> {
    return this.withErrorHandling(
      'findFirst',
      async () => {
        const result = await this.model.findFirst(criteria)
        return result ? this.cleanAuditFields(result) : null
      },
      criteria,
    )
  }

  /** Delega para o delegate Prisma (by, where, _count, etc.) */
  async groupBy(args: Record<string, unknown>): Promise<any[]> {
    return this.withErrorHandling('groupBy', async () => this.model.groupBy(args), args)
  }
}
