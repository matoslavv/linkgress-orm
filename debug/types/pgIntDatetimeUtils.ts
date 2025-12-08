import { Condition, DbColumn, sql, SqlFragment } from "../../src";
import { pgIntDatetime } from "./int-datetime";

const CUSTOM_EPOCH = 1735689600;
export default class PgIntDateTimeUtils {
    static getLocalDay(
        date: DbColumn<Date> | undefined,
        timeZone: string,
        colName: string,
    ): SqlFragment<Date> {
        return sql`((${date} + EXTRACT(TIMEZONE FROM timezone('${sql.raw(timeZone)}', (to_timestamp(${date}) + INTERVAL '${sql.raw(String(CUSTOM_EPOCH))} seconds') AT TIME ZONE 'UTC'))) / 86400)`
            .mapWith((value: number) => PgIntDateTimeUtils.getLocalDayFromNumber(value))
            .as(colName);
    }

    static getLocalDayFromNumber(value: number): Date {
        if (value == null) {
            return null as any;
        }

        return pgIntDatetime.fromDriver(value * 86400) as Date;
    }
}
