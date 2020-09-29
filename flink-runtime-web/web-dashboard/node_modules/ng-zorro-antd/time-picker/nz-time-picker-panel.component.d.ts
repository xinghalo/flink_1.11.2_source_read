import { ChangeDetectorRef, DebugElement, ElementRef, OnDestroy, OnInit, TemplateRef } from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';
import { NzUpdateHostClassService as UpdateCls } from '../core/services/update-host-class.service';
import { NzTimeValueAccessorDirective } from './nz-time-value-accessor.directive';
import { TimeHolder } from './time-holder';
export declare type NzTimePickerUnit = 'hour' | 'minute' | 'second';
export declare class NzTimePickerPanelComponent implements ControlValueAccessor, OnInit, OnDestroy {
    private element;
    private updateCls;
    private cdr;
    private _nzHourStep;
    private _nzMinuteStep;
    private _nzSecondStep;
    private unsubscribe$;
    private onChange;
    private onTouch;
    private _format;
    private _disabledHours;
    private _disabledMinutes;
    private _disabledSeconds;
    private _defaultOpenValue;
    private _opened;
    private _allowEmpty;
    prefixCls: string;
    time: TimeHolder;
    hourEnabled: boolean;
    minuteEnabled: boolean;
    secondEnabled: boolean;
    enabledColumns: number;
    hourRange: ReadonlyArray<{
        index: number;
        disabled: boolean;
    }>;
    minuteRange: ReadonlyArray<{
        index: number;
        disabled: boolean;
    }>;
    secondRange: ReadonlyArray<{
        index: number;
        disabled: boolean;
    }>;
    nzTimeValueAccessorDirective: NzTimeValueAccessorDirective;
    hourListElement: DebugElement;
    minuteListElement: DebugElement;
    secondListElement: DebugElement;
    nzInDatePicker: boolean;
    nzAddOn: TemplateRef<void>;
    nzHideDisabledOptions: boolean;
    nzClearText: string;
    nzPlaceHolder: string;
    nzAllowEmpty: boolean;
    opened: boolean;
    nzDefaultOpenValue: Date;
    nzDisabledHours: () => number[];
    nzDisabledMinutes: (hour: number) => number[];
    nzDisabledSeconds: (hour: number, minute: number) => number[];
    format: string;
    nzHourStep: number;
    nzMinuteStep: number;
    nzSecondStep: number;
    selectInputRange(): void;
    buildHours(): void;
    buildMinutes(): void;
    buildSeconds(): void;
    buildTimes(): void;
    selectHour(hour: {
        index: number;
        disabled: boolean;
    }): void;
    selectMinute(minute: {
        index: number;
        disabled: boolean;
    }): void;
    selectSecond(second: {
        index: number;
        disabled: boolean;
    }): void;
    scrollToSelected(instance: HTMLElement, index: number, duration: number | undefined, unit: NzTimePickerUnit): void;
    translateIndex(index: number, unit: NzTimePickerUnit): number;
    scrollTo(element: HTMLElement, to: number, duration: number): void;
    calcIndex(array: number[], index: number): number;
    protected changed(): void;
    protected touched(): void;
    private setClassMap;
    isSelectedHour(hour: {
        index: number;
        disabled: boolean;
    }): boolean;
    isSelectedMinute(minute: {
        index: number;
        disabled: boolean;
    }): boolean;
    isSelectedSecond(second: {
        index: number;
        disabled: boolean;
    }): boolean;
    initPosition(): void;
    constructor(element: ElementRef, updateCls: UpdateCls, cdr: ChangeDetectorRef);
    ngOnInit(): void;
    ngOnDestroy(): void;
    writeValue(value: Date): void;
    registerOnChange(fn: (value: Date) => void): void;
    registerOnTouched(fn: () => void): void;
}
