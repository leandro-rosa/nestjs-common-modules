/**
 * A utility class that provides helper methods for working with objects.
 *
 * This class includes methods to clean objects by removing properties with null or empty string values.
 * It is useful for ensuring that only meaningful data is processed or sent to external services,
 * especially when dealing with dynamic or optional fields.
 */
export class ObjectHelper {
  /**
   * Removes properties from an object that have null or empty string ('') values.
   *
   * If an array is found, it will only be kept if it is not empty. Otherwise, it is removed.
   *
   * @example
   * const obj = {
   *   name: 'Jesus',
   *   title: null,
   *   description: '',
   *   disciples: ['Pedro', 'João'],
   *   miracles: []
   * };
   *
   * const cleanedObj = ObjectHelper.cleanObject(obj);
   * // { name: 'Jesus', disciples: ['Pedro', 'João'] }
   *
   * @param {Record<string, any>} obj - The object to clean.
   * @returns {Record<string, any>} - A new object with non-null and non-empty string values.
   */
  static cleanObject(obj: Record<string, any>): Record<string, any> {
    const cleanedObj: Record<string, any> = {}
    Object.keys(obj).forEach(key => {
      const value = obj[key]

      if (Array.isArray(value)) {
        if (value.length > 0) {
          cleanedObj[key] = value
        }
        return
      }
      if (value !== null && value !== '') {
        cleanedObj[key] = value
      }
    })

    return cleanedObj
  }
}
