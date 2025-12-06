import { withDatabase, seedTestData } from '../utils/test-database';
import { eq } from '../../src/query/conditions';

describe('Returning Clause', () => {
  describe('insert', () => {
    test('should return full entity by default (backward compatible)', async () => {
      await withDatabase(async (db) => {
        const user = await db.users.insert({
          username: 'newuser',
          email: 'new@test.com',
          isActive: true,
        });

        expect(user).toHaveProperty('id');
        expect(user).toHaveProperty('username', 'newuser');
        expect(user).toHaveProperty('email', 'new@test.com');
        expect(user).toHaveProperty('isActive', true);
        expect(typeof user.id).toBe('number');
      });
    });

    test('should return full entity with returning: true', async () => {
      await withDatabase(async (db) => {
        const user = await db.users.insert(
          { username: 'user1', email: 'user1@test.com', isActive: true },
          { returning: true }
        );

        expect(user).toHaveProperty('id');
        expect(user).toHaveProperty('username', 'user1');
        expect(user).toHaveProperty('email', 'user1@test.com');
      });
    });

    test('should return void with returning: undefined', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insert(
          { username: 'user2', email: 'user2@test.com', isActive: true },
          { returning: undefined }
        );

        expect(result).toBeUndefined();

        // Verify the record was actually inserted
        const users = await db.users.where(u => eq(u.username, 'user2')).toList();
        expect(users.length).toBe(1);
      });
    });

    test('should return selected columns only', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insert(
          { username: 'user3', email: 'user3@test.com', isActive: true },
          { returning: u => ({ id: u.id, username: u.username }) }
        );

        expect(result).toHaveProperty('id');
        expect(result).toHaveProperty('username', 'user3');
        expect(result).not.toHaveProperty('email');
        expect(result).not.toHaveProperty('isActive');
      });
    });

    test('should return single column', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insert(
          { username: 'user4', email: 'user4@test.com', isActive: true },
          { returning: u => ({ generatedId: u.id }) }
        );

        expect(result).toHaveProperty('generatedId');
        expect(typeof result.generatedId).toBe('number');
      });
    });
  });

  describe('insertMany', () => {
    test('should return full entities by default', async () => {
      await withDatabase(async (db) => {
        const users = await db.users.insertMany([
          { username: 'batch1', email: 'batch1@test.com', isActive: true },
          { username: 'batch2', email: 'batch2@test.com', isActive: false },
        ]);

        expect(users.length).toBe(2);
        expect(users[0]).toHaveProperty('id');
        expect(users[0]).toHaveProperty('username');
        expect(users[0]).toHaveProperty('email');
      });
    });

    test('should return void with returning: undefined', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insertMany(
          [
            { username: 'batch3', email: 'batch3@test.com', isActive: true },
            { username: 'batch4', email: 'batch4@test.com', isActive: true },
          ],
          { returning: undefined }
        );

        expect(result).toBeUndefined();

        // Verify records were inserted
        const users = await db.users.where(u => eq(u.username, 'batch3')).toList();
        expect(users.length).toBe(1);
      });
    });

    test('should return selected columns only', async () => {
      await withDatabase(async (db) => {
        const results = await db.users.insertMany(
          [
            { username: 'batch5', email: 'batch5@test.com', isActive: true },
            { username: 'batch6', email: 'batch6@test.com', isActive: true },
          ],
          { returning: u => ({ id: u.id, username: u.username }) }
        );

        expect(results.length).toBe(2);
        expect(results[0]).toHaveProperty('id');
        expect(results[0]).toHaveProperty('username');
        expect(results[0]).not.toHaveProperty('email');
      });
    });
  });

  describe('insertBulk', () => {
    test('should return full entities by default', async () => {
      await withDatabase(async (db) => {
        const users = await db.users.insertBulk([
          { username: 'bulk1', email: 'bulk1@test.com', isActive: true },
        ]);

        expect(users.length).toBe(1);
        expect(users[0]).toHaveProperty('id');
        expect(users[0]).toHaveProperty('username', 'bulk1');
      });
    });

    test('should return void with returning: undefined', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insertBulk(
          [{ username: 'bulk2', email: 'bulk2@test.com', isActive: true }],
          { returning: undefined }
        );

        expect(result).toBeUndefined();
      });
    });

    test('should return selected columns', async () => {
      await withDatabase(async (db) => {
        const results = await db.users.insertBulk(
          [{ username: 'bulk3', email: 'bulk3@test.com', isActive: true }],
          { returning: u => ({ id: u.id }) }
        );

        expect(results.length).toBe(1);
        expect(results[0]).toHaveProperty('id');
        expect(results[0]).not.toHaveProperty('username');
      });
    });
  });

  describe('update', () => {
    test('should return full entities by default', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const updated = await db.users.update(
          { age: 99 },
          u => eq(u.id, users.alice.id)
        );

        expect(updated.length).toBe(1);
        expect(updated[0]).toHaveProperty('id', users.alice.id);
        expect(updated[0]).toHaveProperty('age', 99);
        expect(updated[0]).toHaveProperty('username', 'alice');
      });
    });

    test('should return void with returning: undefined', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const result = await db.users.update(
          { age: 88 },
          u => eq(u.id, users.alice.id),
          { returning: undefined }
        );

        expect(result).toBeUndefined();

        // Verify the update happened
        const alice = await db.users.where(u => eq(u.id, users.alice.id)).toList();
        expect(alice[0].age).toBe(88);
      });
    });

    test('should return selected columns', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const results = await db.users.update(
          { age: 77 },
          u => eq(u.id, users.alice.id),
          { returning: u => ({ id: u.id, age: u.age }) }
        );

        expect(results.length).toBe(1);
        expect(results[0]).toHaveProperty('id', users.alice.id);
        expect(results[0]).toHaveProperty('age', 77);
        expect(results[0]).not.toHaveProperty('username');
      });
    });

    test('should return multiple updated records', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        // Update all active users
        const results = await db.users.update(
          { age: 50 },
          u => eq(u.isActive, true),
          { returning: u => ({ id: u.id, age: u.age }) }
        );

        expect(results.length).toBeGreaterThan(1);
        expect(results.every(r => r.age === 50)).toBe(true);
      });
    });
  });

  describe('bulkUpdate', () => {
    test('should return full entities by default', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const updated = await db.users.bulkUpdate([
          { id: users.alice.id, age: 100 },
          { id: users.bob.id, age: 200 },
        ]);

        expect(updated.length).toBe(2);
        expect(updated[0]).toHaveProperty('id');
        expect(updated[0]).toHaveProperty('username');
        expect(updated[0]).toHaveProperty('email');
      });
    });

    test('should return void with returning: undefined', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const result = await db.users.bulkUpdate(
          [{ id: users.alice.id, age: 111 }],
          { returning: undefined }
        );

        expect(result).toBeUndefined();

        // Verify update happened
        const alice = await db.users.where(u => eq(u.id, users.alice.id)).toList();
        expect(alice[0].age).toBe(111);
      });
    });

    test('should return selected columns', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const results = await db.users.bulkUpdate(
          [
            { id: users.alice.id, age: 33 },
            { id: users.bob.id, age: 44 },
          ],
          { returning: u => ({ id: u.id, age: u.age }) }
        );

        expect(results.length).toBe(2);
        expect(results[0]).toHaveProperty('id');
        expect(results[0]).toHaveProperty('age');
        expect(results[0]).not.toHaveProperty('username');
      });
    });
  });

  describe('upsertBulk', () => {
    test('should return full entities by default (insert)', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const results = await db.users.upsertBulk([
          { username: 'upsert1', email: 'upsert1@test.com', isActive: true },
        ], {
          primaryKey: 'username',
        });

        expect(results.length).toBe(1);
        expect(results[0]).toHaveProperty('id');
        expect(results[0]).toHaveProperty('username', 'upsert1');
        expect(results[0]).toHaveProperty('email');
      });
    });

    test('should return full entities by default (update)', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const results = await db.users.upsertBulk([
          { username: 'alice', email: 'updated@test.com', isActive: false },
        ], {
          primaryKey: 'username',
        });

        expect(results.length).toBe(1);
        expect(results[0]).toHaveProperty('id', users.alice.id);
        expect(results[0]).toHaveProperty('email', 'updated@test.com');
      });
    });

    test('should return void with returning: undefined', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const result = await db.users.upsertBulk(
          [{ username: 'upsert2', email: 'upsert2@test.com', isActive: true }],
          { primaryKey: 'username', returning: undefined }
        );

        expect(result).toBeUndefined();

        // Verify the record was inserted
        const users = await db.users.where(u => eq(u.username, 'upsert2')).toList();
        expect(users.length).toBe(1);
      });
    });

    test('should return selected columns', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const results = await db.users.upsertBulk(
          [{ username: 'upsert3', email: 'upsert3@test.com', isActive: true }],
          {
            primaryKey: 'username',
            returning: u => ({ id: u.id, username: u.username }),
          }
        );

        expect(results.length).toBe(1);
        expect(results[0]).toHaveProperty('id');
        expect(results[0]).toHaveProperty('username', 'upsert3');
        expect(results[0]).not.toHaveProperty('email');
      });
    });
  });

  describe('SQL generation', () => {
    test('insert should not include RETURNING when undefined', async () => {
      await withDatabase(async (db) => {
        const queries: string[] = [];
        const originalQuery = (db as any).client.query.bind((db as any).client);
        (db as any).client.query = async (sql: string, params: any[]) => {
          queries.push(sql);
          return originalQuery(sql, params);
        };

        await db.users.insert(
          { username: 'sqltest1', email: 'sql1@test.com', isActive: true },
          { returning: undefined }
        );

        expect(queries.length).toBe(1);
        expect(queries[0].toUpperCase()).not.toContain('RETURNING');
      });
    });

    test('insert should include RETURNING with selected columns', async () => {
      await withDatabase(async (db) => {
        const queries: string[] = [];
        const originalQuery = (db as any).client.query.bind((db as any).client);
        (db as any).client.query = async (sql: string, params: any[]) => {
          queries.push(sql);
          return originalQuery(sql, params);
        };

        await db.users.insert(
          { username: 'sqltest2', email: 'sql2@test.com', isActive: true },
          { returning: u => ({ id: u.id }) }
        );

        expect(queries.length).toBe(1);
        expect(queries[0].toUpperCase()).toContain('RETURNING');
        expect(queries[0]).toContain('"id"');
      });
    });

    test('update should not include RETURNING when undefined', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const queries: string[] = [];
        const originalQuery = (db as any).client.query.bind((db as any).client);
        (db as any).client.query = async (sql: string, params: any[]) => {
          queries.push(sql);
          return originalQuery(sql, params);
        };

        await db.users.update(
          { age: 50 },
          u => eq(u.id, users.alice.id),
          { returning: undefined }
        );

        expect(queries.some(q => q.toUpperCase().includes('UPDATE'))).toBe(true);
        const updateQuery = queries.find(q => q.toUpperCase().includes('UPDATE'));
        expect(updateQuery!.toUpperCase()).not.toContain('RETURNING');
      });
    });

    test('bulkUpdate should include RETURNING with selected columns', async () => {
      await withDatabase(async (db) => {
        const { users } = await seedTestData(db);

        const queries: string[] = [];
        const originalQuery = (db as any).client.query.bind((db as any).client);
        (db as any).client.query = async (sql: string, params: any[]) => {
          queries.push(sql);
          return originalQuery(sql, params);
        };

        await db.users.bulkUpdate(
          [{ id: users.alice.id, age: 50 }],
          { returning: u => ({ id: u.id, age: u.age }) }
        );

        const updateQuery = queries.find(q => q.toUpperCase().includes('UPDATE'));
        expect(updateQuery).toBeDefined();
        expect(updateQuery!.toUpperCase()).toContain('RETURNING');
      });
    });
  });

  describe('Type safety', () => {
    test('selected returning columns should have correct types', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insert(
          { username: 'typetest', email: 'type@test.com', isActive: true, age: 25 },
          { returning: u => ({ id: u.id, username: u.username, age: u.age, isActive: u.isActive }) }
        );

        // Runtime type checks
        expect(typeof result.id).toBe('number');
        expect(typeof result.username).toBe('string');
        expect(typeof result.age).toBe('number');
        expect(typeof result.isActive).toBe('boolean');
      });
    });
  });

  describe('Edge cases', () => {
    test('empty array insert with returning undefined', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insertMany([], { returning: undefined });
        expect(result).toBeUndefined();
      });
    });

    test('empty array insert with returning true', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.insertMany([], { returning: true });
        expect(result).toEqual([]);
      });
    });

    test('empty array bulkUpdate with returning undefined', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.bulkUpdate([], { returning: undefined });
        expect(result).toBeUndefined();
      });
    });

    test('empty array upsertBulk with returning undefined', async () => {
      await withDatabase(async (db) => {
        const result = await db.users.upsertBulk([], { returning: undefined });
        expect(result).toBeUndefined();
      });
    });
  });
});
