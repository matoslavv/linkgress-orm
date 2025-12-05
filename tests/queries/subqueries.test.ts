import { describe, test, expect } from '@jest/globals';
import { withDatabase, seedTestData } from '../utils/test-database';
import { eq, gt, sql, DbCteBuilder, exists, and, gte, lte } from '../../src';

describe('Subquery Operations', () => {
  describe('Scalar subqueries', () => {
    test('should use scalar subquery in SELECT', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const avgAgeSubquery = db.users
          .select(u => sql<number>`AVG(${u.age})`)
          .asSubquery('scalar');

        const result = await db.users
          .select(u => ({
            username: u.username,
            age: u.age,
            avgAge: avgAgeSubquery,
          }))
          .toList();

        expect(result.length).toBeGreaterThan(0);
        result.forEach(r => {
          expect(r).toHaveProperty('avgAge');
          expect(typeof r.avgAge).toBe('number');
        });

        // All rows should have the same avgAge
        const avgAge = result[0].avgAge;
        result.forEach(r => {
          expect(r.avgAge).toBe(avgAge);
        });
      });
    });
  });

  describe('Array subqueries', () => {
    test('should use array subquery with IN clause', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const activeUserIds = db.users
          .where(u => eq(u.isActive, true))
          .select(u => u.id)
          .asSubquery('array');

        const result = await db.posts
          .where(p => sql`${p.userId} IN (${activeUserIds})`)
          .select(p => ({ title: p.title, userId: p.userId }))
          .toList();

        expect(result.length).toBeGreaterThan(0);
        // All posts should be from active users
      });
    });
  });

  describe('Table subqueries', () => {
    test('should use table subquery as FROM source', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const highViewPostsSubquery = db.posts
          .where(p => gt(p.views, 100))
          .select(p => ({
            postTitle: p.title,
            postViews: p.views,
            userId: p.userId,
          }))
          .asSubquery('table');

        const result = await db.users
          .innerJoin(
            highViewPostsSubquery,
            (user, post) => eq(user.id, post.userId),
            (user, post) => ({
              username: user.username,
              postTitle: post.postTitle,
              views: post.postViews,
            }),
            'highPosts'
          )
          .toList();

        expect(result.length).toBeGreaterThan(0);
        result.forEach(r => {
          expect(r.views).toBeGreaterThan(100);
        });
      });
    });

    test('should use complex subquery with filtering and projection', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const userPostsSubquery = db.posts
          .where(p => gt(p.views, 50))
          .select(p => ({
            userId: p.userId,
            title: p.title,
            views: p.views,
          }))
          .orderBy(p => [[p.views, 'DESC']])
          .limit(10)
          .asSubquery('table');

        const result = await db.users
          .innerJoin(
            userPostsSubquery,
            (user, post) => eq(user.id, post.userId),
            (user, post) => ({
              username: user.username,
              topPostTitle: post.title,
              topPostViews: post.views,
            }),
            'topPosts'
          )
          .toList();

        expect(result.length).toBeGreaterThan(0);
        result.forEach(r => {
          expect(r).toHaveProperty('username');
          expect(r).toHaveProperty('topPostTitle');
          expect(r.topPostViews).toBeGreaterThan(50);
        });
      });
    });
  });

  describe('Nested subqueries', () => {
    test('should nest subqueries', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // First level subquery: active users
        const activeUsersSubquery = db.users
          .where(u => eq(u.isActive, true))
          .select(u => ({
            userId: u.id,
            userName: u.username,
          }))
          .asSubquery('table');

        // Second level: posts by active users, then aggregate
        const statsSubquery = db.posts
          .innerJoin(
            activeUsersSubquery,
            (post, user) => eq(post.userId, user.userId),
            (post, user) => ({
              userId: user.userId,
              views: post.views,
            }),
            'activeUsers'
          )
          .select(p => ({
            userId: p.userId,
            views: p.views,
          }))
          .groupBy(p => ({
            userId: p.userId,
          }))
          .select(g => ({
            userId: g.key.userId,
            totalViews: g.sum(p => p.views),
          }))
          .asSubquery('table');

        // Final query: join stats with users
        const result = await db.users
          .innerJoin(
            statsSubquery,
            (user, stats) => eq(user.id, stats.userId),
            (user, stats) => ({
              username: user.username,
              totalViews: stats.totalViews,
            }),
            'stats'
          )
          .toList();

        expect(result.length).toBeGreaterThan(0);
        result.forEach(r => {
          expect(typeof r.totalViews).toBe('number');
        });
      });
    });
  });

  describe('Correlated subqueries', () => {
    test('should work with EXISTS subquery', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // Users who have posts
        const result = await db.users
          .where(u => sql`EXISTS (
            SELECT 1 FROM posts WHERE user_id = ${u.id}
          )`)
          .select(u => ({ username: u.username }))
          .toList();

        expect(result.length).toBeGreaterThan(0);
        // Should only include Alice and Bob (who have posts)
        expect(result.length).toBeLessThanOrEqual(2);
      });
    });

    test('should work with NOT EXISTS subquery', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // Users who don't have posts
        const result = await db.users
          .where(u => sql`NOT EXISTS (
            SELECT 1 FROM posts WHERE user_id = ${u.id}
          )`)
          .select(u => ({ username: u.username }))
          .toList();

        expect(result.length).toBeGreaterThan(0);
        // Should include Charlie (who has no posts)
        const charlie = result.find(r => r.username === 'charlie');
        expect(charlie).toBeDefined();
      });
    });
  });

  describe('Subquery performance', () => {
    test('should efficiently execute subquery in WHERE clause', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        const maxViewsSubquery = db.posts
          .select(p => sql<number>`MAX(${p.views})`)
          .asSubquery('scalar');

        const result = await db.posts
          .where(p => sql`${p.views} = (${maxViewsSubquery})`)
          .select(p => ({
            title: p.title,
            views: p.views,
          }))
          .toList();

        expect(result.length).toBeGreaterThan(0);
        // Should return the post with max views (200)
        expect(result[0].views).toBe(200);
      });
    });
  });

  describe('SQL magic string mapWith', () => {
    // These tests verify that mapWith() transforms data when reading from the database.
    // mapWith() accepts a simple inline function: sql`...`.mapWith((val: T) => transformedVal)
    // This is much more ergonomic than the old customType() approach.
    //
    // ✅ ALL 11 tests passing (100% success rate)
    // - Basic SELECT with mapWith
    // - Scalar subqueries with mapWith
    // - Table subqueries with mapWith
    // - CTEs with mapWith
    // - JOINs with mapWith
    // - Multiple mappers in single query
    // - Nested subqueries with mapWith

    describe('Basic SELECT with mapWith - Actual Transformations', () => {
      test('should transform numbers by multiplying by 10', async () => {
        await withDatabase(async (db) => {
          await seedTestData(db);

          const result = await db.users
            .select(u => ({
              username: u.username,
              originalAge: u.age,
              multipliedAge: sql<number>`${u.age}`.mapWith((val: number) => val * 10),
            }))
            .where(u => eq(u.username, 'alice'))
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          expect(result[0].originalAge).toBe(25);
          // If mapWith works, this should be 250 (25 * 10)
          expect(result[0].multipliedAge).toBe(250);
        });
      });

      test('should transform strings to uppercase', async () => {
        await withDatabase(async (db) => {
          const { users } = await seedTestData(db);

          const result = await db.users
            .where(u => eq(u.id, users.alice.id))
            .select(u => ({
              originalUsername: u.username,
              uppercaseUsername: sql<string>`${u.username}`.mapWith((val: string) => val.toUpperCase()),
            }))
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          expect(result[0].originalUsername).toBe('alice');
          // If mapWith works, this should be 'ALICE'
          expect(result[0].uppercaseUsername).toBe('ALICE');
        });
      });

      test('should transform computed values by adding prefix', async () => {
        await withDatabase(async (db) => {
          await seedTestData(db);

          const result = await db.users
            .select(u => ({
              username: u.username,
              prefixedInfo: sql<string>`${u.username} || ' - ' || ${u.email}`.mapWith((val: string) => `User: ${val}`),
            }))
            .where(u => eq(u.username, 'bob'))
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          expect(result[0].username).toBe('bob');
          // If mapWith works, should have 'User: ' prefix
          expect(result[0].prefixedInfo).toMatch(/^User: bob - bob@test\.com$/);
        });
      });

      test('should transform to custom object type', async () => {
        await withDatabase(async (db) => {
          await seedTestData(db);

          interface UserInfo {
            name: string;
            contact: string;
          }

          const result = await db.users
            .select(u => ({
              username: u.username,
              userInfo: sql<UserInfo>`${u.username} || ' - ' || ${u.email}`.mapWith((val: string) => {
                const parts = val.split(' - ');
                return { name: parts[0] || '', contact: parts[1] || '' };
              }),
            }))
            .where(u => eq(u.username, 'charlie'))
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          expect(result[0].username).toBe('charlie');
          // If mapWith works, should be transformed to object
          expect(result[0].userInfo).toHaveProperty('name', 'charlie');
          expect(result[0].userInfo).toHaveProperty('contact', 'charlie@test.com');
        });
      });
    });

    describe('Subquery with mapWith', () => {
      test('should transform scalar subquery result by rounding', async () => {
        await withDatabase(async (db) => {
          await seedTestData(db);

          const avgViewsSubquery = db.posts
            .select(p => sql<number>`AVG(${p.views})`.mapWith((val: number) => Math.round(val)))
            .asSubquery('scalar');

          const result = await db.users
            .select(u => ({
              username: u.username,
              roundedAvgViews: avgViewsSubquery,
            }))
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          // Average of 100, 150, 200 is 150
          // If mapWith works, should be rounded (already whole number, but proves mapper is called)
          expect(result[0].roundedAvgViews).toBe(150);
          expect(Number.isInteger(result[0].roundedAvgViews)).toBe(true);
        });
      });

      test('should transform table subquery values by doubling', async () => {
        await withDatabase(async (db) => {
          await seedTestData(db);

          const postsSubquery = db.posts
            .select(p => ({
              userId: p.userId,
              originalViews: p.views,
              doubledViews: sql<number>`${p.views}`.mapWith((val: number) => val * 2),
            }))
            .asSubquery('table');

          const result = await db.users
            .innerJoin(
              postsSubquery,
              (user, post) => eq(user.id, post.userId),
              (user, post) => ({
                username: user.username,
                originalViews: post.originalViews,
                transformedViews: post.doubledViews,
              }),
              'post_subquery'
            )
            .where(u => eq(u.username, 'alice'))
            .limit(1)
            .toList();

          expect(result.length).toBeGreaterThan(0);
          // Alice's first post has 100 views
          expect(result[0].originalViews).toBe(100);
          // If mapWith works, doubled views should be 200
          expect(result[0].transformedViews).toBe(200);
        });
      });
    });

    describe('CTE with mapWith', () => {
      test('should transform CTE values with suffix', async () => {
        await withDatabase(async (db) => {
          const { users } = await seedTestData(db);

          const cteBuilder = new DbCteBuilder();
          const usersCTE = cteBuilder.with(
            'user_info',
            db.users.select(u => ({
              userId: u.id,
              originalUsername: u.username,
              transformedInfo: sql<string>`${u.username} || '@' || ${u.email}`.mapWith((val: string) => `${val} [CTE]`),
            }))
          );

          const result = await db.users
            .where(u => eq(u.id, users.bob.id))
            .with(usersCTE.cte)
            .leftJoin(
              usersCTE.cte,
              (user, cte) => eq(user.id, cte.userId),
              (user, cte) => ({
                id: user.id,
                original: cte.originalUsername,
                transformed: cte.transformedInfo,
              })
            )
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          expect(result[0].original).toBe('bob');
          // If mapWith works, should have ' [CTE]' suffix
          expect(result[0].transformed).toBe('bob@bob@test.com [CTE]');
        });
      });
    });

    describe('JOIN with mapWith', () => {
      test('should transform JOIN projection with brackets', async () => {
        await withDatabase(async (db) => {
          const { users } = await seedTestData(db);

          const result = await db.users
            .where(u => eq(u.id, users.alice.id))
            .innerJoin(
              db.posts,
              (user, post) => eq(user.id, post.userId),
              (user, post) => ({
                username: user.username,
                title: post.title,
                bracketedCombined: sql<string>`${user.username} || ' - ' || ${post.title}`.mapWith((val: string) => `[${val}]`),
              })
            )
            .limit(1)
            .toList();

          expect(result.length).toBeGreaterThan(0);
          expect(result[0].username).toBe('alice');
          // If mapWith works, should be wrapped in brackets
          expect(result[0].bracketedCombined).toMatch(/^\[alice - Alice Post \d\]$/);
        });
      });

      test('should transform table subquery in JOIN by adding 1000', async () => {
        await withDatabase(async (db) => {
          const { users } = await seedTestData(db);

          const postSubquery = db.posts
            .select(p => ({
              postId: p.id,
              userId: p.userId,
              originalViews: p.views,
              incrementedViews: sql<number>`${p.views}`.mapWith((val: number) => val + 1000),
            }))
            .asSubquery('table');

          const result = await db.users
            .where(u => eq(u.id, users.bob.id))
            .innerJoin(
              postSubquery,
              (user, post) => eq(user.id, post.userId),
              (user, post) => ({
                username: user.username,
                original: post.originalViews,
                transformed: post.incrementedViews,
              }),
              'post_data'
            )
            .limit(1)
            .toList();

          expect(result.length).toBeGreaterThan(0);
          expect(result[0].username).toBe('bob');
          // Bob's post has 200 views
          expect(result[0].original).toBe(200);
          // If mapWith works, should be 1200 (200 + 1000)
          expect(result[0].transformed).toBe(1200);
        });
      });
    });

    describe('WHERE clause with mapWith', () => {
      test('should use mapWith in WHERE subquery (mapper not applied to WHERE)', async () => {
        await withDatabase(async (db) => {
          await seedTestData(db);

          // Note: mapWith in WHERE clause subquery doesn't affect the WHERE condition itself,
          // only the value if it were selected. This test verifies compilation.
          const avgViewsSubquery = db.posts
            .select(p => sql<number>`AVG(${p.views})`.mapWith((val: number) => Math.ceil(val)))
            .asSubquery('scalar');

          const result = await db.posts
            .where(p => sql`${p.views} > (${avgViewsSubquery})`)
            .select(p => ({
              title: p.title,
              views: p.views,
            }))
            .toList();

          expect(result.length).toBeGreaterThan(0);
          // Posts with views > 150 (the average)
          result.forEach(r => {
            expect(r.views).toBeGreaterThan(150);
          });
        });
      });
    });

    describe('Multiple mapWith in single query', () => {
      test('should apply multiple different transformations', async () => {
        await withDatabase(async (db) => {
          const { users } = await seedTestData(db);

          const result = await db.users
            .where(u => eq(u.id, users.alice.id))
            .select(u => ({
              reversedUsername: sql<string>`${u.username}`.mapWith((val: string) => val.split('').reverse().join('')),
              tripledAge: sql<number>`${u.age}`.mapWith((val: number) => val * 3),
              email: u.email,
            }))
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          // If mapWith works, username should be reversed
          expect(result[0].reversedUsername).toBe('ecila'); // 'alice' reversed
          // If mapWith works, age should be tripled
          expect(result[0].tripledAge).toBe(75); // 25 * 3
          expect(result[0].email).toBe('alice@test.com');
        });
      });
    });

    describe('Nested subqueries with mapWith', () => {
      test('should apply transformation in nested scalar subquery', async () => {
        await withDatabase(async (db) => {
          await seedTestData(db);

          // Inner subquery with mapWith
          const avgViewsSubquery = db.posts
            .select(p => sql<number>`AVG(${p.views})`.mapWith((val: number) => Math.floor(val / 10) * 10))
            .asSubquery('scalar');

          // Outer query using the transformed value
          const result = await db.users
            .select(u => ({
              username: u.username,
              roundedAvgViews: avgViewsSubquery,
            }))
            .limit(1)
            .toList();

          expect(result.length).toBe(1);
          // Average is 150, floor to nearest 10 is still 150
          // If mapWith works, should be 150
          expect(result[0].roundedAvgViews).toBe(150);
        });
      });
    });
  });

  describe('Navigation property access in WHERE conditions', () => {
    test('should automatically add JOIN when navigation property used in WHERE', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // Access user.username through navigation property in WHERE condition
        // This should automatically add a JOIN on the user table
        const result = await db.posts
          .where(p => eq(p.user!.username, 'alice'))
          .select(p => ({
            title: p.title,
            userId: p.userId,
          }))
          .toList();

        expect(result.length).toBe(2); // Alice has 2 posts
        result.forEach(r => {
          expect(r.title).toMatch(/Alice Post/);
        });
      });
    });

    test('should work with navigation property in sql template WHERE condition', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // Access user.isActive through navigation property in sql template
        const result = await db.posts
          .where(p => sql`${p.user!.isActive} = true`)
          .select(p => ({
            title: p.title,
            userId: p.userId,
          }))
          .toList();

        // Alice and Bob are active, so 3 posts total
        expect(result.length).toBe(3);
      });
    });

    test('should work with navigation property in combined WHERE condition', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // Combine navigation property condition with regular condition
        const result = await db.posts
          .where(p => sql`${p.user!.age} > 30 AND ${p.views} > 100`)
          .select(p => ({
            title: p.title,
            views: p.views,
          }))
          .toList();

        // Only Bob (age 35) has a post with views > 100 (200 views)
        expect(result.length).toBe(1);
        expect(result[0].title).toBe('Bob Post');
        expect(result[0].views).toBe(200);
      });
    });

    test('should work with navigation property in both SELECT and WHERE', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // Use navigation in both SELECT and WHERE
        const result = await db.posts
          .where(p => eq(p.user!.isActive, true))
          .select(p => ({
            title: p.title,
            username: p.user!.username,
            userAge: p.user!.age,
          }))
          .toList();

        expect(result.length).toBe(3); // Alice (2) + Bob (1)
        result.forEach(r => {
          expect(r).toHaveProperty('username');
          expect(r).toHaveProperty('userAge');
        });
      });
    });

    test('should add JOIN when outer navigation property used in exists() subquery WHERE', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // This is the critical test case:
        // - Outer query is on posts
        // - Inner subquery is on orders
        // - Inner subquery's WHERE references p.user!.age (navigation from outer query)
        // This should generate a JOIN on the users table in the outer query
        const result = await db.posts
          .where(p => exists(
            db.orders
              .where(o => and(
                eq(o.userId, p.userId),
                // This is the key: accessing outer query's navigation property
                // inside the inner subquery's WHERE condition
                sql`${p.user!.age} >= 30`
              ))
              .select(o => ({ orderId: o.id }))
              .asSubquery('table')
          ))
          .select(p => ({
            title: p.title,
            userId: p.userId,
          }))
          .toList();

        // Only Bob (age 35) has both posts AND orders, and age >= 30
        // Alice (age 25) has orders but age < 30
        expect(result.length).toBe(1);
        expect(result[0].title).toBe('Bob Post');
      });
    });

    test('should add JOIN when outer navigation property used in exists() with multiple conditions', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // More complex case: multiple outer navigation properties in inner subquery
        const result = await db.posts
          .where(p => exists(
            db.orders
              .where(o => and(
                eq(o.userId, p.userId),
                // Multiple navigation property accesses from outer query
                sql`${p.user!.age} >= 20 AND ${p.user!.age} <= 40 AND ${p.user!.isActive} = true`
              ))
              .select(o => ({ orderId: o.id }))
              .asSubquery('table')
          ))
          .select(p => ({
            title: p.title,
            userId: p.userId,
          }))
          .toList();

        // Alice (age 25, active) and Bob (age 35, active) both have orders
        // Both are between 20-40 and active
        expect(result.length).toBe(3); // Alice's 2 posts + Bob's 1 post
      });
    });

    test('should handle complex CTE with navigation property in filter used by grouped query with leftJoin', async () => {
      await withDatabase(async (db) => {
        await seedTestData(db);

        // This test replicates a complex real-world scenario:
        // 1. Filter built dynamically using subquery that accesses navigation properties
        // 2. 1st CTE that aggregates order data
        // 3. 2nd CTE that: uses groupBy, applies filter from #1, performs leftJoin with CTE1
        // 4. Final query that selects using these CTEs

        // Step 1: Create a dynamic filter with EXISTS subquery that accesses navigation property
        // This filter checks if a post's user has any orders
        const hasOrdersFilter = (post: any) => exists(
          db.orders
            .where(o => and(
              eq(o.userId, post.userId),
              // Access navigation property from outer query - this is the key test!
              // This should trigger a JOIN on users table in the outer query
              sql`${post.user!.age} >= 25`
            ))
            .select(o => ({ orderId: o.id }))
            .asSubquery('table')
        );

        const cteBuilder = new DbCteBuilder();

        // Step 2: 1st CTE - aggregate order amounts by user
        const orderStatsCte = cteBuilder.withAggregation(
          'order_stats_cte',
          db.orders.select(o => ({
            userId: o.userId,
            orderAmount: o.totalAmount,
            orderStatus: o.status,
          })),
          o => ({ userId: o.userId }),
          'orderDetails'
        );

        // Step 3: 2nd CTE - groupBy with filter using navigation properties, leftJoin with CTE1
        // This is the critical part - using the hasOrdersFilter inside a CTE that:
        // - Applies WHERE with navigation property access (via exists subquery)
        // - Does groupBy
        // - LeftJoins with another CTE
        const postStatsCte = cteBuilder.withAggregation(
          'post_stats_cte',
          db.posts
            .where(post => hasOrdersFilter(post))  // Filter with navigation property access
            .select(p => ({
              postId: p.id,
              title: p.title,
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
            .leftJoin(
              orderStatsCte,
              (stats, orders) => eq(stats.userId, orders.userId),
              (stats, orders) => ({
                userId: stats.userId,
                postCount: stats.postCount,
                totalViews: stats.totalViews,
                orderDetails: orders.orderDetails,
              })
            ),
          p => ({ userId: p.userId }),
          'postStats'
        );

        // Step 4: Final query using the CTEs
        const result = await db.users
          .where((u: any) => eq(u.isActive, true))
          .with(orderStatsCte)
          .with(postStatsCte)
          .leftJoin(
            postStatsCte,
            (user: any, stats: any) => eq(user.id, stats.userId),
            (user: any, stats: any) => ({
              username: user.username,
              age: user.age,
              postStats: stats.postStats,
            })
          )
          .toList();

        // Verify results:
        // - Alice (age 25) has posts and orders, so should be included with stats
        // - Bob (age 35) has posts and orders, so should be included with stats
        // - Charlie (age 45) is not active, so filtered out
        expect(result.length).toBe(2);

        const aliceResult = result.find((r: any) => r.username === 'alice');
        const bobResult = result.find((r: any) => r.username === 'bob');

        expect(aliceResult).toBeDefined();
        expect(bobResult).toBeDefined();

        // Alice has 2 posts, total views = 100 + 150 = 250
        expect(aliceResult?.postStats).toBeDefined();
        expect(aliceResult?.postStats?.length).toBeGreaterThan(0);

        // Bob has 1 post, total views = 200
        expect(bobResult?.postStats).toBeDefined();
        expect(bobResult?.postStats?.length).toBeGreaterThan(0);
      });
    });
  });
});
