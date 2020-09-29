import { ChangeDetectorRef, ElementRef, Renderer2 } from '@angular/core';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { CascaderOption } from './nz-cascader-definitions';
export declare class NzCascaderOptionComponent {
    private sanitizer;
    private cdr;
    option: CascaderOption;
    activated: boolean;
    highlightText: string;
    nzLabelProperty: string;
    constructor(sanitizer: DomSanitizer, cdr: ChangeDetectorRef, elementRef: ElementRef, renderer: Renderer2);
    getOptionLabel(): string;
    renderHighlightString(str: string): SafeHtml;
    markForCheck(): void;
}
