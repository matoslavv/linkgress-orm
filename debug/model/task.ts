import { DbEntity, DbColumn } from "../../src";
import type { TaskLevel } from "./taskLevel";

export class Task extends DbEntity {
  id!: DbColumn<number>;
  title!: DbColumn<string>;
  status!: DbColumn<'pending' | 'processing' | 'completed' | 'cancelled'>;
  priority!: DbColumn<'low' | 'medium' | 'high'>;
  levelId?: DbColumn<number>;

  // Navigation property
  level?: TaskLevel;
}
