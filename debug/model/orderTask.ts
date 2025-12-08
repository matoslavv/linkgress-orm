import { DbColumn, DbEntity } from "../../src";
import type { HourMinute } from "../types/hour-minute";
import { Order } from "./order";
import { Task } from "./task";

export class OrderTask extends DbEntity {
    orderId!: DbColumn<number>;
    taskId!: DbColumn<number>;
    sortOrder?: DbColumn<number>;

    // Navigation property
	order?: Order;
	task?: Task;
}