import { EventEmitter } from '@angular/core';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
export declare class TimePickerButtonComponent {
    locale: NzCalendarI18nInterface;
    timePickerDisabled: boolean;
    showTimePicker: boolean;
    readonly showTimePickerChange: EventEmitter<boolean>;
    prefixCls: string;
    onClick(): void;
}
