/**
 * @name when
 *
 * @description
 * Execute a function and return the result when a condition is true, otherwise return the original object
 *
 * ```javascript [playground]
 * import when from 'https://unpkg.com/rubico/dist/x/when.es.js'
 *
 * const isEven = (num) => num % 2 === 0;

 * const doubleIfEven = when(
 *  isEven,
 *  (num) => num * 2
 * );

 * console.log(doubleIfEven(100)) // 200
 * console.log(doubleIfEven(101)) // 101
 * ```
 */

const when = (predicate, fn) => function when(object) {
    if (predicate(object)) {
      return fn(object);
    }
    return object;
  };

module.exports = when;
