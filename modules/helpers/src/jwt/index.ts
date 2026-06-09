import * as jwt from 'jsonwebtoken'

export class JwtHelper {
  /**
   * @description
   * Extract email from JWT (Bearer) payload without verifying signature.
   * @param authorization - The Authorization header value, expected to be in the format
   */
  static getEmailFromAuthorizationHeader(authorization: string | string[] | undefined) {
    if (!authorization || Array.isArray(authorization)) return null
    const trimmed = authorization.trim()
    if (!trimmed.toLowerCase().startsWith('bearer ')) return null
    const token = trimmed.slice(7).trim()
    if (!token) return null
    const decoded = jwt.decode(token) as { email?: string } | null
    if (!decoded || typeof decoded.email !== 'string' || !decoded.email.trim()) {
      return null
    }

    return decoded
  }
}
