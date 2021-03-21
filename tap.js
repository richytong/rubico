const isPromise = require('./_internal/isPromise')
const always = require('./_internal/always')
const tapSync = require('./_internal/tapSync')
const thunkifyArgs = require('./_internal/thunkifyArgs')
const thunkConditional = require('./_internal/thunkConditional')
const curry3 = require('./_internal/curry3')
const __ = require('./_internal/placeholder')

/**
 * @name tap
 *
 * @synopsis
 * ```coffeescript [specscript]
 * var args ...any,
 *   tapper ...args=>Promise|any
 *
 * tap(tapper)(...args) -> Promise|args[0]
 * ```
 *
 * @description
 * Call a function with a value, returning the value. Promises created by the tapper are resolved before returning the value.
 *
 * ```javascript [playground]
 * pipe([
 *   tap(console.log),
 *   value => value + 'bar',
 *   tap(console.log),
 * ])('foo') // 'foo'
 *           // 'foobar'
 * ```
 */
const tap = func => function tapping(...args) {
  const result = args[0],
    call = func(...args)
  return isPromise(call) ? call.then(always(result)) : result
}

/**
 * @name tap.sync
 *
 * @synopsis
 * ```coffeescript [specscript]
 * var args ...any,
 *   tapper ...args=>any
 *
 * tap.sync(tapper)(...args) -> args[0]
 * ```
 *
 * @description
 * Synchronous `tap`
 *
 * ```javascript [playground]
 * pipe([
 *   tap.sync(number => console.log('square', number ** 2)),
 *   tap.sync(number => console.log('cube', number ** 3)),
 * ])(3) // 9
 *       // 27
 * ```
 */
tap.sync = tapSync

/**
 * @name tap.if
 *
 * @synopsis
 * ```coffeescript [specscript]
 * var args ...any,
 *   predicate ...args=>Promise|boolean,
 *   tapper ...args=>Promise|any
 *
 * tap.if(predicate, tapper)(...args) -> Promise|args[0]
 * ```
 *
 * @description
 * Conditional `tap` by predicate
 *
 * ```javascript [playground]
 * const isOdd = number => number % 2 == 1
 *
 * const logIfOdd = tap.if(
 *   isOdd,
 *   number => console.log(number, 'is an odd number'))
 *
 * logIfOdd(2)
 * logIfOdd(3) // 3 is an odd number
 * ```
 *
 * @related tap
 */
tap.if = (predicate, func) => function tappingIf(...args) {
  const predication = predicate(...args)
  if (isPromise(predication)) {
    return predication.then(curry3(
      thunkConditional, __, thunkifyArgs(tap(func), args), always(args[0])))
  }
  if (predication) {
    const execution = func(...args)
    if (isPromise(execution)) {
      return execution.then(always(args[0]))
    }
  }
  return args[0]
}

/**
 * @name tap.once
 *
 * @synopsis
 * ```coffeescript [specscript]
 * tap.once(func function) -> onlyRunsOnce function
 * ```
 *
 * @description
 * `tap` that executes a function only once.
 */
tap.once = function tapOnce(func) {
  let didCall = false
  return function tappingOnce(...args) {
    if (!didCall) {
      didCall = true
      return func(...args)
    }
  }
}

module.exports = tap
