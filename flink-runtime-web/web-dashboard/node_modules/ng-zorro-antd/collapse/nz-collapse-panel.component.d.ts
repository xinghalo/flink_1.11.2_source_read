import { ChangeDetectorRef, ElementRef, EventEmitter, OnDestroy, OnInit, Renderer2, TemplateRef } from '@angular/core';
import { NzCollapseComponent } from './nz-collapse.component';
export declare class NzCollapsePanelComponent implements OnInit, OnDestroy {
    private cdr;
    private nzCollapseComponent;
    nzActive: boolean;
    nzDisabled: boolean;
    nzShowArrow: boolean;
    nzExtra: string | TemplateRef<void>;
    nzHeader: string | TemplateRef<void>;
    nzExpandedIcon: string | TemplateRef<void>;
    readonly nzActiveChange: EventEmitter<boolean>;
    clickHeader(): void;
    markForCheck(): void;
    constructor(cdr: ChangeDetectorRef, nzCollapseComponent: NzCollapseComponent, elementRef: ElementRef, renderer: Renderer2);
    ngOnInit(): void;
    ngOnDestroy(): void;
}
