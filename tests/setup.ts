import 'dotenv/config';
import { disposeSharedDatabase } from './utils/test-database';

/**
 * Global test setup - runs before all tests
 */
beforeAll(async () => {
  // Ensure we're using test database
  const dbName = process.env.DB_NAME || 'linkgress_test';
  if (!dbName.includes('test')) {
    throw new Error('Tests must use a test database! Set DB_NAME to include "test" in the name.');
  }
});

/**
 * Cleanup after all tests - close the shared database connection
 */
afterAll(async () => {
  await disposeSharedDatabase();
});

/**
 * Extend Jest matchers with custom assertions
 */
expect.extend({
  toBeWithinRange(received: number, floor: number, ceiling: number) {
    const pass = received >= floor && received <= ceiling;
    if (pass) {
      return {
        message: () => `expected ${received} not to be within range ${floor} - ${ceiling}`,
        pass: true,
      };
    } else {
      return {
        message: () => `expected ${received} to be within range ${floor} - ${ceiling}`,
        pass: false,
      };
    }
  },
});

declare global {
  namespace jest {
    interface Matchers<R> {
      toBeWithinRange(floor: number, ceiling: number): R;
    }
  }
}
