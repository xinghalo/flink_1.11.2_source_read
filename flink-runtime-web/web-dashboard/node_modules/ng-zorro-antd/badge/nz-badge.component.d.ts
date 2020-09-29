import { AfterViewInit, ElementRef, OnChanges, OnInit, Renderer2, SimpleChanges, TemplateRef } from '@angular/core';
export declare type NzBadgeStatusType = 'success' | 'processing' | 'default' | 'error' | 'warning';
export declare class NzBadgeComponent implements OnInit, AfterViewInit, OnChanges {
    private renderer;
    private elementRef;
    maxNumberArray: string[];
    countArray: number[];
    countSingleArray: number[];
    count: number;
    contentElement: ElementRef;
    nzShowZero: boolean;
    nzShowDot: boolean;
    nzDot: boolean;
    nzOverflowCount: number;
    nzText: string;
    nzStyle: {
        [key: string]: string;
    };
    nzStatus: NzBadgeStatusType;
    nzCount: number | TemplateRef<void>;
    checkContent(): void;
    readonly showSup: boolean;
    generateMaxNumberArray(): void;
    constructor(renderer: Renderer2, elementRef: ElementRef);
    ngOnInit(): void;
    ngAfterViewInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
}
