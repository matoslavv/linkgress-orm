import { DbColumn, DbEntity } from "../../src";
import type { User } from "./user";

export class TaskLevel extends DbEntity {
    id!: DbColumn<number>;
    name!: DbColumn<string>;
    createdById!: DbColumn<number>;

    // Navigation property
    createdBy?: User;
}
