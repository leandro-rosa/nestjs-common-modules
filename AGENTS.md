# NestJS Common Modules

This repository contains reusable NestJS modules.

## Project Standards

- Code must be written in English.
- Use TypeScript only.
- Do not use `any`.
- Follow SOLID, Clean Code, and NestJS best practices.
- Prefer small, reusable modules.
- Every public service, module, DTO, interface, and helper must be exported through `src/index.ts`.
- Avoid circular dependencies.
- Keep modules independent from application-specific business rules.
- Do not add inline comments.
- Use JSDoc only for public classes, methods, and functions.

## Repository Structure

- `modules/aws`
- `modules/cache`
- `modules/elasticsearch`
- `modules/health`
- `modules/helpers`
- `modules/hold-it`
- `modules/http-client`
- `modules/logger`
- `modules/prisma-db-client`
- `modules/sheeter`

## Validation Before Commit

Always run:

```bash
npm run lint
npm run build
npm run test
```
