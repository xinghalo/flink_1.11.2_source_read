import { EventEmitter } from '@angular/core';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
export declare class OkButtonComponent {
    locale: NzCalendarI18nInterface;
    okDisabled: boolean;
    readonly clickOk: EventEmitter<void>;
    prefixCls: string;
}
