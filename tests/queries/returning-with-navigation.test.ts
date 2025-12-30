import { describe, test, expect, beforeAll, afterAll } from '@jest/globals';
import { createTestDatabase, setupDatabase, cleanupDatabase, seedTestData } from '../utils/test-database';
import { eq, and, gt } from '../../src';
import { AppDatabase } from '../../debug/schema/appDatabase';

describe('Navigation properties in RETURNING statements', () => {
  let db: AppDatabase;

  beforeAll(async () => {
    db = createTestDatabase({ logQueries: false });
    await setupDatabase(db);
  });

  afterAll(async () => {
    await cleanupDatabase(db);
  });

  describe('delete().returning() with navigation', () => {
    test('should return navigation property fields from delete', async () => {
      // Setup: Create data
      const user = await db.users.insert({
        username: 'delete_nav_test',
        email: 'delete_nav@test.com',
        age: 30,
        isActive: true,
      }).returning();

      const order = await db.orders.insert({
        userId: user.id,
        status: 'pending',
        totalAmount: 99.99,
      }).returning();

      // Delete order and return navigation data
      const deleted = await db.orders
        .where(o => eq(o.id, order.id))
        .delete()
        .returning(o => ({
          orderId: o.id,
          orderStatus: o.status,
          userName: o.user!.username,
          userEmail: o.user!.email,
        }));

      expect(deleted.length).toBe(1);
      expect(deleted[0].orderId).toBe(order.id);
      expect(deleted[0].orderStatus).toBe('pending');
      expect(deleted[0].userName).toBe('delete_nav_test');
      expect(deleted[0].userEmail).toBe('delete_nav@test.com');

      // Cleanup
      await db.users.where(u => eq(u.id, user.id)).delete();
    });

    test('should work with delete without navigation (simple RETURNING)', async () => {
      const user = await db.users.insert({
        username: 'delete_simple_test',
        email: 'delete_simple@test.com',
        age: 25,
        isActive: true,
      }).returning();

      const deleted = await db.users
        .where(u => eq(u.id, user.id))
        .delete()
        .returning(u => ({
          id: u.id,
          username: u.username,
        }));

      expect(deleted.length).toBe(1);
      expect(deleted[0].id).toBe(user.id);
      expect(deleted[0].username).toBe('delete_simple_test');
    });
  });

  describe('update().returning() with navigation', () => {
    test('should return navigation property fields from update', async () => {
      // Setup
      const user = await db.users.insert({
        username: 'update_nav_test',
        email: 'update_nav@test.com',
        age: 30,
        isActive: true,
      }).returning();

      const post = await db.posts.insert({
        title: 'Original Title',
        content: 'Content',
        userId: user.id,
        views: 0,
        publishTime: { hour: 10, minute: 30 },
      }).returning();

      // Update post and return navigation data
      const updated = await db.posts
        .where(p => eq(p.id, post.id))
        .update({ title: 'Updated Title', views: 100 })
        .returning(p => ({
          postId: p.id,
          postTitle: p.title,
          authorName: p.user!.username,
          authorEmail: p.user!.email,
        }));

      expect(updated.length).toBe(1);
      expect(updated[0].postId).toBe(post.id);
      expect(updated[0].postTitle).toBe('Updated Title');
      expect(updated[0].authorName).toBe('update_nav_test');
      expect(updated[0].authorEmail).toBe('update_nav@test.com');

      // Cleanup
      await db.posts.where(p => eq(p.id, post.id)).delete();
      await db.users.where(u => eq(u.id, user.id)).delete();
    });

    test('should work with update without navigation (simple RETURNING)', async () => {
      const user = await db.users.insert({
        username: 'update_simple_test',
        email: 'update_simple@test.com',
        age: 25,
        isActive: true,
      }).returning();

      const updated = await db.users
        .where(u => eq(u.id, user.id))
        .update({ age: 35 })
        .returning(u => ({
          id: u.id,
          age: u.age,
        }));

      expect(updated.length).toBe(1);
      expect(updated[0].id).toBe(user.id);
      expect(updated[0].age).toBe(35);

      // Cleanup
      await db.users.where(u => eq(u.id, user.id)).delete();
    });
  });

  describe('insert().returning() with navigation', () => {
    test('should return navigation property fields from insert', async () => {
      // Setup user first
      const user = await db.users.insert({
        username: 'insert_nav_test',
        email: 'insert_nav@test.com',
        age: 30,
        isActive: true,
      }).returning();

      // Insert post and return navigation data
      const inserted = await db.posts
        .insert({
          title: 'New Post',
          content: 'New Content',
          userId: user.id,
          views: 50,
          publishTime: { hour: 14, minute: 0 },
        })
        .returning(p => ({
          postId: p.id,
          postTitle: p.title,
          authorName: p.user!.username,
          authorAge: p.user!.age,
        }));

      expect(inserted).toBeDefined();
      expect(inserted.postTitle).toBe('New Post');
      expect(inserted.authorName).toBe('insert_nav_test');
      expect(inserted.authorAge).toBe(30);

      // Cleanup
      await db.posts.where(p => eq(p.id, inserted.postId)).delete();
      await db.users.where(u => eq(u.id, user.id)).delete();
    });

    test('should work with insert without navigation (simple RETURNING)', async () => {
      const inserted = await db.users
        .insert({
          username: 'insert_simple_test',
          email: 'insert_simple@test.com',
          age: 25,
          isActive: true,
        })
        .returning(u => ({
          id: u.id,
          username: u.username,
        }));

      expect(inserted).toBeDefined();
      expect(inserted.username).toBe('insert_simple_test');

      // Cleanup
      await db.users.where(u => eq(u.id, inserted.id)).delete();
    });
  });

  describe('insertBulk().returning() with navigation', () => {
    test('should return navigation property fields from bulk insert', async () => {
      // Setup user first
      const user = await db.users.insert({
        username: 'bulk_nav_test',
        email: 'bulk_nav@test.com',
        age: 40,
        isActive: true,
      }).returning();

      // Bulk insert posts and return navigation data
      const inserted = await db.posts
        .insertBulk([
          { title: 'Bulk Post 1', content: 'Content 1', userId: user.id, views: 10, publishTime: { hour: 8, minute: 0 } },
          { title: 'Bulk Post 2', content: 'Content 2', userId: user.id, views: 20, publishTime: { hour: 9, minute: 0 } },
        ])
        .returning(p => ({
          postId: p.id,
          postTitle: p.title,
          authorName: p.user!.username,
        }));

      expect(inserted.length).toBe(2);
      // Order may vary, so check both have the author name and titles are present
      const titles = inserted.map(i => i.postTitle).sort();
      expect(titles).toEqual(['Bulk Post 1', 'Bulk Post 2']);
      expect(inserted[0].authorName).toBe('bulk_nav_test');
      expect(inserted[1].authorName).toBe('bulk_nav_test');

      // Cleanup
      await db.posts.where(p => eq(p.userId, user.id)).delete();
      await db.users.where(u => eq(u.id, user.id)).delete();
    });

    test('should work with bulk insert without navigation (simple RETURNING)', async () => {
      const inserted = await db.users
        .insertBulk([
          { username: 'bulk1', email: 'bulk1@test.com', age: 20, isActive: true },
          { username: 'bulk2', email: 'bulk2@test.com', age: 21, isActive: true },
        ])
        .returning(u => ({
          id: u.id,
          username: u.username,
        }));

      expect(inserted.length).toBe(2);
      expect(inserted[0].username).toBe('bulk1');
      expect(inserted[1].username).toBe('bulk2');

      // Cleanup
      await db.users.where(u => eq(u.username, 'bulk1')).delete();
      await db.users.where(u => eq(u.username, 'bulk2')).delete();
    });
  });

  describe('upsertBulk().returning() with navigation', () => {
    test('should return navigation property fields from upsert', async () => {
      // Setup user first
      const user = await db.users.insert({
        username: 'upsert_nav_test',
        email: 'upsert_nav@test.com',
        age: 50,
        isActive: true,
      }).returning();

      // Insert initial order
      const initialOrder = await db.orders.insert({
        userId: user.id,
        status: 'pending',
        totalAmount: 50.00,
      }).returning();

      // Upsert order and return navigation data
      const upserted = await db.orders
        .upsertBulk([
          { id: initialOrder.id, userId: user.id, status: 'completed', totalAmount: 75.00 },
        ], { primaryKey: 'id' })
        .returning(o => ({
          orderId: o.id,
          orderStatus: o.status,
          ownerName: o.user!.username,
        }));

      expect(upserted.length).toBe(1);
      expect(upserted[0].orderId).toBe(initialOrder.id);
      expect(upserted[0].orderStatus).toBe('completed');
      expect(upserted[0].ownerName).toBe('upsert_nav_test');

      // Cleanup
      await db.orders.where(o => eq(o.id, initialOrder.id)).delete();
      await db.users.where(u => eq(u.id, user.id)).delete();
    });

    test('should work with upsert without navigation (simple RETURNING)', async () => {
      const user = await db.users.insert({
        username: 'upsert_simple_test',
        email: 'upsert_simple@test.com',
        age: 30,
        isActive: true,
      }).returning();

      const order = await db.orders.insert({
        userId: user.id,
        status: 'pending',
        totalAmount: 100.00,
      }).returning();

      const upserted = await db.orders
        .upsertBulk([
          { id: order.id, userId: user.id, status: 'processing', totalAmount: 120.00 },
        ], { primaryKey: 'id' })
        .returning(o => ({
          id: o.id,
          status: o.status,
        }));

      expect(upserted.length).toBe(1);
      expect(upserted[0].id).toBe(order.id);
      expect(upserted[0].status).toBe('processing');

      // Cleanup
      await db.orders.where(o => eq(o.id, order.id)).delete();
      await db.users.where(u => eq(u.id, user.id)).delete();
    });
  });

  describe('returning() with single-level navigation', () => {
    test('should support single-level navigation in returning (orderTask -> task)', async () => {
      // Setup
      const user = await db.users.insert({
        username: 'singlelevel_test',
        email: 'singlelevel@test.com',
        age: 35,
        isActive: true,
      }).returning();

      const taskLevel = await db.taskLevels.insert({
        name: 'Critical',
        createdById: user.id,
      }).returning();

      const task = await db.tasks.insert({
        title: 'Single Level Task',
        status: 'pending',
        priority: 'high',
        levelId: taskLevel.id,
      }).returning();

      const order = await db.orders.insert({
        userId: user.id,
        status: 'pending',
        totalAmount: 200.00,
      }).returning();

      // Insert orderTask and return single-level navigation
      const inserted = await db.orderTasks
        .insert({
          orderId: order.id,
          taskId: task.id,
          sortOrder: 1,
        })
        .returning(ot => ({
          orderId: ot.orderId,
          taskId: ot.taskId,
          taskTitle: ot.task!.title,
          taskStatus: ot.task!.status,
        }));

      expect(inserted).toBeDefined();
      expect(inserted.orderId).toBe(order.id);
      expect(inserted.taskId).toBe(task.id);
      expect(inserted.taskTitle).toBe('Single Level Task');
      expect(inserted.taskStatus).toBe('pending');

      // Cleanup
      await db.orderTasks.where(ot => and(eq(ot.orderId, order.id), eq(ot.taskId, task.id))).delete();
      await db.orders.where(o => eq(o.id, order.id)).delete();
      await db.tasks.where(t => eq(t.id, task.id)).delete();
      await db.taskLevels.where(tl => eq(tl.id, taskLevel.id)).delete();
      await db.users.where(u => eq(u.id, user.id)).delete();
    });
  });

  describe('returning(true) - full entity', () => {
    test('returning(true) should still work without CTE (no navigation)', async () => {
      const inserted = await db.users
        .insert({
          username: 'full_entity_test',
          email: 'full_entity@test.com',
          age: 28,
          isActive: true,
        })
        .returning();

      expect(inserted).toBeDefined();
      expect(inserted.id).toBeDefined();
      expect(inserted.username).toBe('full_entity_test');
      expect(inserted.email).toBe('full_entity@test.com');
      expect(inserted.age).toBe(28);
      expect(inserted.isActive).toBe(true);

      // Cleanup
      await db.users.where(u => eq(u.id, inserted.id)).delete();
    });
  });

  describe('edge cases', () => {
    test('should handle null navigation property gracefully (LEFT JOIN)', async () => {
      // Create user without any related data that would be joined
      const user = await db.users.insert({
        username: 'null_nav_test',
        email: 'null_nav@test.com',
        age: 30,
        isActive: true,
      }).returning();

      // Create post
      const post = await db.posts.insert({
        title: 'Post with user',
        content: 'Content',
        userId: user.id,
        views: 0,
        publishTime: { hour: 12, minute: 0 },
      }).returning();

      // Delete and return navigation - should work
      const deleted = await db.posts
        .where(p => eq(p.id, post.id))
        .delete()
        .returning(p => ({
          postId: p.id,
          authorName: p.user!.username,
        }));

      expect(deleted.length).toBe(1);
      expect(deleted[0].postId).toBe(post.id);
      expect(deleted[0].authorName).toBe('null_nav_test');

      // Cleanup
      await db.users.where(u => eq(u.id, user.id)).delete();
    });

    test('should handle multiple rows with navigation in delete', async () => {
      // Setup
      const user = await db.users.insert({
        username: 'multi_delete_test',
        email: 'multi_delete@test.com',
        age: 30,
        isActive: true,
      }).returning();

      // Create multiple posts
      await db.posts.insertBulk([
        { title: 'Delete Me 1', content: 'Content 1', userId: user.id, views: 10, publishTime: { hour: 8, minute: 0 } },
        { title: 'Delete Me 2', content: 'Content 2', userId: user.id, views: 20, publishTime: { hour: 9, minute: 0 } },
        { title: 'Delete Me 3', content: 'Content 3', userId: user.id, views: 30, publishTime: { hour: 10, minute: 0 } },
      ]);

      // Delete all and return navigation
      const deleted = await db.posts
        .where(p => eq(p.userId, user.id))
        .delete()
        .returning(p => ({
          postTitle: p.title,
          authorName: p.user!.username,
        }));

      expect(deleted.length).toBe(3);
      deleted.forEach(d => {
        expect(d.authorName).toBe('multi_delete_test');
        expect(d.postTitle).toContain('Delete Me');
      });

      // Cleanup
      await db.users.where(u => eq(u.id, user.id)).delete();
    });
  });
});
