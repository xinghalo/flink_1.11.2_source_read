import { ChangeDetectorRef, EventEmitter, OnChanges, OnDestroy, OnInit, SimpleChanges, TemplateRef } from '@angular/core';
import { NzI18nService } from '../i18n/nz-i18n.service';
export declare class NzPaginationComponent implements OnInit, OnDestroy, OnChanges {
    private i18n;
    private cdr;
    locale: any;
    firstIndex: number;
    pages: number[];
    private $destroy;
    readonly nzPageSizeChange: EventEmitter<number>;
    readonly nzPageIndexChange: EventEmitter<number>;
    nzShowTotal: TemplateRef<{
        $implicit: number;
        range: [number, number];
    }>;
    nzInTable: boolean;
    nzSize: 'default' | 'small';
    nzPageSizeOptions: number[];
    nzItemRender: TemplateRef<{
        $implicit: 'page' | 'prev' | 'next';
        page: number;
    }>;
    nzShowSizeChanger: boolean;
    nzHideOnSinglePage: boolean;
    nzShowQuickJumper: boolean;
    nzSimple: boolean;
    nzTotal: number;
    nzPageIndex: number;
    nzPageSize: number;
    validatePageIndex(value: number): number;
    updatePageIndexValue(page: number): void;
    isPageIndexValid(value: number): boolean;
    jumpPage(index: number): void;
    jumpDiff(diff: number): void;
    onPageSizeChange($event: number): void;
    handleKeyDown(_: KeyboardEvent, input: HTMLInputElement, clearInputValue: boolean): void;
    /** generate indexes list */
    buildIndexes(): void;
    readonly lastIndex: number;
    readonly isLastIndex: boolean;
    readonly isFirstIndex: boolean;
    readonly ranges: number[];
    readonly showAddOption: boolean;
    constructor(i18n: NzI18nService, cdr: ChangeDetectorRef);
    ngOnInit(): void;
    ngOnDestroy(): void;
    ngOnChanges(changes: SimpleChanges): void;
}
