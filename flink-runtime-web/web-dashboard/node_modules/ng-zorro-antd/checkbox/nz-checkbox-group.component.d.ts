import { FocusMonitor } from '@angular/cdk/a11y';
import { ChangeDetectorRef, ElementRef, OnInit, Renderer2 } from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';
export interface NzCheckBoxOptionInterface {
    label: string;
    value: string;
    checked?: boolean;
    disabled?: boolean;
}
export declare class NzCheckboxGroupComponent implements ControlValueAccessor, OnInit {
    private elementRef;
    private focusMonitor;
    private cdr;
    onChange: (value: any) => void;
    onTouched: () => any;
    options: NzCheckBoxOptionInterface[];
    nzDisabled: boolean;
    onOptionChange(): void;
    trackByOption(_index: number, option: NzCheckBoxOptionInterface): string;
    constructor(elementRef: ElementRef, focusMonitor: FocusMonitor, cdr: ChangeDetectorRef, renderer: Renderer2);
    ngOnInit(): void;
    writeValue(value: NzCheckBoxOptionInterface[]): void;
    registerOnChange(fn: (_: NzCheckBoxOptionInterface[]) => {}): void;
    registerOnTouched(fn: () => {}): void;
    setDisabledState(isDisabled: boolean): void;
}
