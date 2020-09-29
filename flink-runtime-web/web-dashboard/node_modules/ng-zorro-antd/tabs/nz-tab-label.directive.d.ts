import { ElementRef, Renderer2 } from '@angular/core';
export declare class NzTabLabelDirective {
    elementRef: ElementRef;
    disabled: boolean;
    constructor(elementRef: ElementRef, renderer: Renderer2);
    getOffsetLeft(): number;
    getOffsetWidth(): number;
    getOffsetTop(): number;
    getOffsetHeight(): number;
}
