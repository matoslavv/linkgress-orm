import { describe, test, expect } from '@jest/globals';
import { withDatabase, seedTestData } from '../utils/test-database';
import { eq, gt } from '../../src';
import { assertType } from '../utils/type-tester';

describe('firstOrDefault()', () => {
  describe('Navigation Collections', () => {
    test('should return first item from collection with firstOrDefault()', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const usersWithTopPost = await db.users
          .select(u => ({
            userId: u.id,
            username: u.username,
            topPost: u.posts!
              .orderBy(p => [[p.views, 'DESC']])
              .select(p => ({
                title: p.title,
                views: p.views,
              }))
              .firstOrDefault(),
          }))
          .toList();

        expect(usersWithTopPost.length).toBeGreaterThan(0);

        // Alice should have her highest viewed post
        const alice = usersWithTopPost.find(u => u.username === 'alice');
        expect(alice).toBeDefined();
        expect(alice!.topPost).not.toBeNull();
        expect(alice!.topPost!.title).toBeDefined();
        expect(alice!.topPost!.views).toBe(150); // Alice Post 2 has 150 views

        // Verify it's a single object or null, not an array
        expect(Array.isArray(alice!.topPost)).toBe(false);
      });
    });

    test('should return null when collection is empty with firstOrDefault()', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const usersWithFilteredPost = await db.users
          .select(u => ({
            userId: u.id,
            username: u.username,
            // Filter for posts with impossible view count
            impossiblePost: u.posts!
              .where(p => gt(p.views!, 999999))
              .select(p => ({
                title: p.title,
                views: p.views,
              }))
              .firstOrDefault(),
          }))
          .toList();

        expect(usersWithFilteredPost.length).toBeGreaterThan(0);

        // All users should have null for impossiblePost
        usersWithFilteredPost.forEach(u => {
          expect(u.impossiblePost).toBeNull();
        });
      });
    });

    test('should return null for users without any posts', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const usersWithFirstPost = await db.users
          .select(u => ({
            userId: u.id,
            username: u.username,
            firstPost: u.posts!
              .select(p => ({
                title: p.title,
              }))
              .firstOrDefault(),
          }))
          .toList();

        // Charlie has no posts, so his firstPost should be null
        const charlie = usersWithFirstPost.find(u => u.username === 'charlie');
        expect(charlie).toBeDefined();
        expect(charlie!.firstPost).toBeNull();

        // Alice has posts, so she should have a firstPost
        const alice = usersWithFirstPost.find(u => u.username === 'alice');
        expect(alice).toBeDefined();
        expect(alice!.firstPost).not.toBeNull();
      });
    });

    test('should work with where clause before firstOrDefault()', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const usersWithHighViewPost = await db.users
          .select(u => ({
            userId: u.id,
            username: u.username,
            highViewPost: u.posts!
              .where(p => gt(p.views!, 100))
              .orderBy(p => [[p.views, 'DESC']])
              .select(p => ({
                title: p.title,
                views: p.views,
              }))
              .firstOrDefault(),
          }))
          .toList();

        const alice = usersWithHighViewPost.find(u => u.username === 'alice');
        expect(alice).toBeDefined();
        // Alice's highest viewed post > 100 is "Alice Post 2" with 150 views
        expect(alice!.highViewPost).not.toBeNull();
        expect(alice!.highViewPost!.views).toBe(150);
      });
    });

    test('should work with firstOrDefault() on full entity (no select)', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const usersWithLatestPost = await db.users
          .select(u => ({
            userId: u.id,
            username: u.username,
            latestPost: u.posts!
              .orderBy(p => [[p.id, 'DESC']])
              .firstOrDefault(),
          }))
          .toList();

        const alice = usersWithLatestPost.find(u => u.username === 'alice');
        expect(alice).toBeDefined();
        expect(alice!.latestPost).not.toBeNull();
        expect(alice!.latestPost!.title).toBeDefined();
        expect(alice!.latestPost!.userId).toBe(alice!.userId);
      });
    });
  });

  describe('GroupBy Queries', () => {
    test('should use firstOrDefault() on grouped query', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const result = await db.posts
          .select(p => ({
            userId: p.userId,
            views: p.views,
          }))
          .groupBy(p => ({
            userId: p.userId,
          }))
          .select(g => ({
            userId: g.key.userId,
            postCount: g.count(),
          }))
          .orderBy(g => [[g.userId, 'DESC']])
          .firstOrDefault();

        // Should return a single result or null, not an array
        expect(result).not.toBeNull();
        if (result !== null) {
          expect(typeof result.userId).toBe('number');
          expect(typeof result.postCount).toBe('number');
          assertType<{ userId: number; postCount: number }, typeof result>(result);
        }
      });
    });

    test('should return null from grouped query when no results', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const result = await db.posts
          .where(p => gt(p.views!, 999999)) // No posts with this many views
          .select(p => ({
            userId: p.userId,
            views: p.views,
          }))
          .groupBy(p => ({
            userId: p.userId,
          }))
          .select(g => ({
            userId: g.key.userId,
            postCount: g.count(),
          }))
          .firstOrDefault();

        expect(result).toBeNull();
      });
    });

    test('should use firstOrDefault() with aggregates', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const result = await db.posts
          .select(p => ({
            userId: p.userId,
            views: p.views,
          }))
          .groupBy(p => ({
            userId: p.userId,
          }))
          .select(g => ({
            userId: g.key.userId,
            postCount: g.count(),
            totalViews: g.sum(p => p.views),
          }))
          .firstOrDefault();

        expect(result).not.toBeNull();
        expect(typeof result!.postCount).toBe('number');
        expect(typeof result!.totalViews).toBe('number');
      });
    });
  });

  describe('Regular Queries (existing behavior)', () => {
    test('should work on regular entity query', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const user = await db.users
          .where(u => eq(u.username, 'alice'))
          .firstOrDefault();

        expect(user).not.toBeNull();
        expect(user!.username).toBe('alice');
      });
    });

    test('should return null when no match found', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const user = await db.users
          .where(u => eq(u.username, 'nonexistent'))
          .firstOrDefault();

        expect(user).toBeNull();
      });
    });
  });
});
