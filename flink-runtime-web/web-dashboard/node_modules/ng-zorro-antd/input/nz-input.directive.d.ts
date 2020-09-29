import { ElementRef, Renderer2 } from '@angular/core';
import { NgControl } from '@angular/forms';
import { NzSizeLDSType } from '../core/types/size';
export declare class NzInputDirective {
    ngControl: NgControl;
    private _disabled;
    nzSize: NzSizeLDSType;
    disabled: boolean;
    constructor(ngControl: NgControl, renderer: Renderer2, elementRef: ElementRef);
}
