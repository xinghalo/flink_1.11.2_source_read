import { ChangeDetectorRef, ElementRef, Injector, NgZone, OnDestroy, OnInit, Renderer2, TemplateRef } from '@angular/core';
import { Params } from '@angular/router';
export declare const NZ_ROUTE_DATA_BREADCRUMB = "breadcrumb";
export interface BreadcrumbOption {
    label: string;
    params: Params;
    url: string;
}
export declare class NzBreadCrumbComponent implements OnInit, OnDestroy {
    private injector;
    private ngZone;
    private cd;
    nzAutoGenerate: boolean;
    nzSeparator: string | TemplateRef<void>;
    breadcrumbs: BreadcrumbOption[] | undefined;
    private destroy$;
    constructor(injector: Injector, ngZone: NgZone, cd: ChangeDetectorRef, elementRef: ElementRef, renderer: Renderer2);
    ngOnInit(): void;
    ngOnDestroy(): void;
    navigate(url: string, e: MouseEvent): void;
    private getBreadcrumbs;
}
