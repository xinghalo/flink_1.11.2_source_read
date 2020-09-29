import { FocusMonitor } from '@angular/cdk/a11y';
import { AfterViewInit, ChangeDetectorRef, TemplateRef } from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';
import { NzSizeDSType } from '../core/types/size';
export declare class NzSwitchComponent implements ControlValueAccessor, AfterViewInit {
    private cdr;
    private focusMonitor;
    checked: boolean;
    onChange: (value: boolean) => void;
    onTouched: () => void;
    private switchElement;
    nzLoading: boolean;
    nzDisabled: boolean;
    nzControl: boolean;
    nzCheckedChildren: string | TemplateRef<void>;
    nzUnCheckedChildren: string | TemplateRef<void>;
    nzSize: NzSizeDSType;
    hostClick(e: MouseEvent): void;
    updateValue(value: boolean): void;
    onKeyDown(e: KeyboardEvent): void;
    focus(): void;
    blur(): void;
    constructor(cdr: ChangeDetectorRef, focusMonitor: FocusMonitor);
    ngAfterViewInit(): void;
    writeValue(value: boolean): void;
    registerOnChange(fn: (_: boolean) => void): void;
    registerOnTouched(fn: () => void): void;
    setDisabledState(isDisabled: boolean): void;
}
