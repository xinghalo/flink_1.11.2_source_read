import { ElementRef, Renderer2 } from '@angular/core';
import { Observable } from 'rxjs';
export declare class NzDropDownDirective {
    elementRef: ElementRef;
    private renderer;
    el: HTMLElement;
    hover$: Observable<boolean>;
    $click: Observable<boolean>;
    setDisabled(disabled: boolean): void;
    constructor(elementRef: ElementRef, renderer: Renderer2);
}
