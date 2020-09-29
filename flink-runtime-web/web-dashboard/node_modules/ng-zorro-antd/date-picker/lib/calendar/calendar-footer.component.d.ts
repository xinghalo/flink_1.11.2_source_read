import { EventEmitter, TemplateRef } from '@angular/core';
import { isNonEmptyString, isTemplateRef } from '../../../core/util/check';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
import { CandyDate } from '../candy-date';
export declare class CalendarFooterComponent {
    locale: NzCalendarI18nInterface;
    showToday: boolean;
    hasTimePicker: boolean;
    isRange: boolean;
    showTimePicker: boolean;
    readonly showTimePickerChange: EventEmitter<boolean>;
    timePickerDisabled: boolean;
    okDisabled: boolean;
    disabledDate: (d: Date) => boolean;
    extraFooter: TemplateRef<void> | string;
    rangeQuickSelector: TemplateRef<void>;
    readonly clickOk: EventEmitter<void>;
    readonly clickToday: EventEmitter<CandyDate>;
    prefixCls: string;
    isTemplateRef: typeof isTemplateRef;
    isNonEmptyString: typeof isNonEmptyString;
}
