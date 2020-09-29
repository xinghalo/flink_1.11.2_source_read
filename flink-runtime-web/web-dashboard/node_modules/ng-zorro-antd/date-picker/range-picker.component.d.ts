import { ChangeDetectorRef, ElementRef, Renderer2 } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzI18nService } from '../i18n/nz-i18n.service';
import { DateHelperService } from '../i18n/date-helper.service';
import { DateRangePickerComponent } from './date-range-picker.component';
export declare class NzRangePickerComponent extends DateRangePickerComponent {
    noAnimation?: NzNoAnimationDirective | undefined;
    isRange: boolean;
    constructor(i18n: NzI18nService, cdr: ChangeDetectorRef, dateHelper: DateHelperService, renderer: Renderer2, elementRef: ElementRef, noAnimation?: NzNoAnimationDirective | undefined);
}
