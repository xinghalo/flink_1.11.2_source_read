import { DisabledDateFn, DisabledTimeConfig, DisabledTimeFn } from '../standard-types';
import { CandyDate } from './candy-date';
export declare function getTimeConfig(value: CandyDate, disabledTime: DisabledTimeFn): DisabledTimeConfig;
export declare function isTimeValidByConfig(value: CandyDate, disabledTimeConfig: DisabledTimeConfig): boolean;
export declare function isTimeValid(value: CandyDate, disabledTime: DisabledTimeFn): boolean;
export declare function isAllowedDate(value: CandyDate, disabledDate?: DisabledDateFn, disabledTime?: DisabledTimeFn): boolean;
