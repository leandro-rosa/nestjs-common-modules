/**
 * A utility class for common string manipulations such as formatting, cleaning, and normalizing text.
 *
 * The `StringHelper` class provides several static methods for tasks like:
 * - Capitalizing the first letter of a string.
 * - Removing HTML tags from a string.
 * - Cleaning and formatting multi-line text by removing extra spaces, carriage returns, and newlines.
 * - Normalizing field names by converting them to camelCase and removing special characters or accents.
 */
export class StringHelper {
  /**
   * Capitalizes the first letter of a given string and converts the rest of the string to lowercase.
   *
   * @example
   * StringHelper.capitalizeFirstLetter('bárbara') // 'Bárbara'
   * StringHelper.capitalizeFirstLetter('DEUS') // 'Deus'
   *
   * If the input string is not provided, it returns an empty string.
   *
   * @param {string} [str] - The string to capitalize. Optional, defaults to an empty string if not provided.
   * @returns {string} - The string with the first letter capitalized and the remaining letters in lowercase.
   */
  static capitalizeFirstLetter(str?: string): string {
    if (!str) return ''
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase()
  }

  /**
   * Capitalizes the first letter of each word in a given string and converts the rest of each word to lowercase.
   *
   * @example
   * StringHelper.capitalizeEachWord('bárbara dos santos') // 'Bárbara Dos Santos'
   * StringHelper.capitalizeEachWord('MEU DEUS') // 'Meu Deus'
   *
   * If the input string is not provided, it returns an empty string.
   *
   * @param {string} [str] - The string to capitalize each word. Optional, defaults to an empty string if not provided.
   * @returns {string} - The string with the first letter of each word capitalized and the remaining letters in lowercase.
   */
  static capitalizeEachWord(str?: string): string {
    if (!str) return ''

    return str
      .split(' ')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' ')
  }

  /**
   * Removes HTML tags from a string, leaving only plain text.
   *
   * @example
   * StringHelper.removeHtmlTags('<p>Jesus é amor!</p>') // 'Jesus é amor!'
   * StringHelper.removeHtmlTags('<div>Deus é fiel</div>') // 'Deus é fiel'
   *
   * @param {string} htmlString - The string containing HTML to clean.
   * @returns {string} - The plain text string without HTML tags.
   */
  static removeHtmlTags(htmlString: string): string {
    return htmlString.replace(/<\/?[^>]+(>|$)/g, '')
  }

  /**
   * Cleans a string by removing carriage returns (\r), newlines (\n), excessive spacing,
   * and empty lines. Returns the string broken into an array of clean, trimmed lines.
   *
   * @example
   * const rawString = 'Bryan está aqui\n\nJesus é amor\r\n\r\nCauã está brincando';
   * StringHelper.cleanAndFormatText(rawString)
   * // ['Bryan está aqui', 'Jesus é amor', 'Cauã está brincando']
   *
   * @param {string} text - The raw text to clean and format.
   * @returns {string[]} - The cleaned and formatted text as an array of trimmed lines.
   */
  static cleanAndFormatText(text: string): string[] {
    return text
      .replace(/\r/g, '') // Remove carriage returns (\r)
      .split('\n') // Split by newlines (\n)
      .map(line => line.trim()) // Trim extra spaces
      .filter(line => line.length > 0) // Remove empty lines
      .map(line => line.replace(/\s\s+/g, ' ')) // Replace multiple spaces with a single space
  }

  /**
   * Normalizes a field name by converting it to lowercase, removing special characters,
   * and converting it to camelCase. Handles accented characters and special letters like 'ç'.
   *
   * @example
   * StringHelper.normalizeFieldName('Jesus é amor') // 'jesusEAmor'
   * StringHelper.normalizeFieldName('Bárbara & Cauã - Família') // 'barbaraCauaFamilia'
   *
   * @param {string} fieldName - The name of the field to normalize.
   * @returns {string} - The normalized field name.
   */
  static normalizeFieldName(fieldName: string): string {
    return fieldName
      .normalize('NFD') // Normalize accented characters
      .replace(/[\u0300-\u036f]/g, '') // Remove accents
      .replace(/ç/g, 'c') // Replace 'ç' with 'c'
      .toLowerCase() // Convert the whole string to lowercase first
      .replace(/[^\w\s]/g, '') // Remove special characters (except spaces and underscores)
      .replace(/\s+/g, '_') // Replace spaces with underscores
      .replace(/(?:_|^)(\w)/g, (_, c) => (c ? c.toUpperCase() : '')) // Capitalize letters after underscores
      .replace(/^\w/, c => c.toLowerCase()) // Ensure the first letter is lowercase
  }

  /**
   * Normalizes a field value by converting it to lowercase, removing space, accents and special characters,
   *
   * @example
   * StringHelper.normalizeFieldName('Jesus é amor') // 'jesuseamor'
   * StringHelper.normalizeFieldName('Bárbara & Cauã - Família') // 'barbaracauafamilia'
   *
   * @param {string} fieldName - The value of the field to normalize.
   * @returns {string} - The normalized field name.
   */
  static normalizeFieldValue(fieldValue: string): string {
    return fieldValue
      .normalize('NFD') // Normalize accented characters
      .replace(/[\u0300-\u036f]/g, '') // Remove accents
      .replace(/ç/g, 'c') // Replace 'ç' with 'c'
      .toLowerCase() // Convert the whole string to lowercase first
      .replace(/[^\w\s]/g, '') // Remove special characters (except spaces and underscores)
      .replace(/\s+/g, '') // Remove spaces
      .replace(/_/g, '') // Remove underscores
  }

  static escapePrismaLikeUnderscore(value: string) {
    return value.replace(/\\/g, '\\\\').replace(/_/g, '\\_')
  }

  /**
   * Converts a given string into snake_case.
   *
   * Handles different cases like:
   * - "NomeDoAtributo" → "nome_do_atributo"
   * - "Nome do Atributo" → "nome_do_atributo"
   * - "nomeDoAtributo" → "nome_do_atributo"
   *
   * @example
   * StringHelper.toSnakeCase('NomeDoAtributo') // 'nome_do_atributo'
   * StringHelper.toSnakeCase('Nome do Atributo') // 'nome_do_atributo'
   * StringHelper.toSnakeCase('nomeDoAtributo') // 'nome_do_atributo'
   *
   * @param {string} value - The input string.
   * @returns {string} - The string converted to snake_case.
   */
  static toSnakeCase(value: string): string {
    if (!value) return ''

    return value
      .normalize('NFD') // remove acentos
      .replace(/[\u0300-\u036f]/g, '') // strip diacritics
      .replace(/ç/g, 'c')
      .replace(/([a-z])([A-Z])/g, '$1_$2') // camelCase -> snake_case
      .replace(/[\s\-]+/g, '_') // espaços e hifens -> underscore
      .replace(/[^a-zA-Z0-9_]/g, '') // remove caracteres especiais
      .toLowerCase()
      .replace(/_{2,}/g, '_') // evita underscores duplicados
      .replace(/^_+|_+$/g, '') // remove underscores extras no início/fim
  }

  /**
   * @description
   * Normalizes an EAN/GTIN by removing left padding zeros while preserving "0" when the string is all zeros.
   * This is useful to match identifiers that may come either padded or unpadded.
   *
   * @param value - Raw EAN/GTIN string
   * @returns Normalized string without leading zeros
   */
  static normalizeEanNoLeftZeros(value: string): string {
    const trimmed = value.trim()
    const noZeros = trimmed.replace(/^0+/, '')
    return noZeros.length ? noZeros : '0'
  }
}
