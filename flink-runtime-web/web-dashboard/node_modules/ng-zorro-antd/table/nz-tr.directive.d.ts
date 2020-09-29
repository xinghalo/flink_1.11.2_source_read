import { ElementRef, Renderer2 } from '@angular/core';
import { NzTableComponent } from './nz-table.component';
export declare class NzTrDirective {
    private elementRef;
    private renderer;
    nzTableComponent: NzTableComponent;
    nzExpand: boolean;
    constructor(elementRef: ElementRef, renderer: Renderer2, nzTableComponent: NzTableComponent);
}
