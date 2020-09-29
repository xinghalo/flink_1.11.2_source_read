import { ChangeDetectorRef, OnChanges, OnDestroy, OnInit, SimpleChanges, TemplateRef } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { NzI18nService } from '../i18n/nz-i18n.service';
export declare class NzEmptyComponent implements OnChanges, OnInit, OnDestroy {
    private sanitizer;
    private i18n;
    private cdr;
    nzNotFoundImage: string | TemplateRef<void>;
    nzNotFoundContent: string | TemplateRef<void>;
    nzNotFoundFooter: string | TemplateRef<void>;
    defaultSvg: import("@angular/platform-browser").SafeResourceUrl;
    isContentString: boolean;
    locale: {
        [key: string]: string;
    };
    readonly shouldRenderContent: boolean;
    private destroy$;
    constructor(sanitizer: DomSanitizer, i18n: NzI18nService, cdr: ChangeDetectorRef);
    ngOnChanges(changes: SimpleChanges): void;
    ngOnInit(): void;
    ngOnDestroy(): void;
}
