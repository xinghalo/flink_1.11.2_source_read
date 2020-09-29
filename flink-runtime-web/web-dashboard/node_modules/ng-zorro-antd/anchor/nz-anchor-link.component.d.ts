import { ChangeDetectorRef, ElementRef, OnDestroy, OnInit, Renderer2, TemplateRef } from '@angular/core';
import { NzAnchorComponent } from './nz-anchor.component';
export declare class NzAnchorLinkComponent implements OnInit, OnDestroy {
    elementRef: ElementRef;
    private anchorComp;
    private cdr;
    nzHref: string;
    titleStr: string | null;
    titleTpl: TemplateRef<void>;
    active: boolean;
    nzTitle: string | TemplateRef<void>;
    nzTemplate: TemplateRef<void>;
    constructor(elementRef: ElementRef, anchorComp: NzAnchorComponent, cdr: ChangeDetectorRef, renderer: Renderer2);
    ngOnInit(): void;
    goToClick(e: Event): void;
    markForCheck(): void;
    ngOnDestroy(): void;
}
