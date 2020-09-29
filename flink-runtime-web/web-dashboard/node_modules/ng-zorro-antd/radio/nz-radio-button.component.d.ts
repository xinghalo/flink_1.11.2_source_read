import { ChangeDetectorRef, ElementRef, Renderer2 } from '@angular/core';
import { FocusMonitor } from '@angular/cdk/a11y';
import { NzRadioComponent } from './nz-radio.component';
export declare class NzRadioButtonComponent extends NzRadioComponent {
    constructor(elementRef: ElementRef, renderer: Renderer2, cdr: ChangeDetectorRef, focusMonitor: FocusMonitor);
}
