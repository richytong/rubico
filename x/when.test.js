const assert = require('assert');
const when = require('./when');

const isEven = num => num % 2 === 0;

describe('when', () => {
  it('happy path', async () => {
    const doubleIfEven = when(isEven, num => num * 2);

    assert.strictEqual(doubleIfEven(100), 200);
    assert.strictEqual(doubleIfEven(101), 101);
  });
});
