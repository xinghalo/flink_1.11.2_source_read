import { ElementRef } from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';
import { DateHelperService } from '../i18n/date-helper.service';
export declare class NzTimeValueAccessorDirective implements ControlValueAccessor {
    private dateHelper;
    private elementRef;
    private _onChange;
    private _onTouch;
    nzTime: string;
    keyup(): void;
    blur(): void;
    changed(): void;
    touched(): void;
    setRange(): void;
    constructor(dateHelper: DateHelperService, elementRef: ElementRef);
    writeValue(value: Date): void;
    registerOnChange(fn: (value: Date) => void): void;
    registerOnTouched(fn: () => void): void;
}
