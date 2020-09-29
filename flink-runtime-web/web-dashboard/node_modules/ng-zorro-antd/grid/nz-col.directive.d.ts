import { AfterViewInit, ElementRef, OnChanges, OnDestroy, OnInit, Renderer2 } from '@angular/core';
import { Subject } from 'rxjs';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzRowDirective } from './nz-row.directive';
export interface EmbeddedProperty {
    span: number;
    pull: number;
    push: number;
    offset: number;
    order: number;
}
export declare class NzColDirective implements OnInit, OnChanges, AfterViewInit, OnDestroy {
    private nzUpdateHostClassService;
    private elementRef;
    nzRowDirective: NzRowDirective;
    renderer: Renderer2;
    private el;
    private prefixCls;
    protected destroy$: Subject<{}>;
    nzSpan: number;
    nzOrder: number;
    nzOffset: number;
    nzPush: number;
    nzPull: number;
    nzXs: number | EmbeddedProperty;
    nzSm: number | EmbeddedProperty;
    nzMd: number | EmbeddedProperty;
    nzLg: number | EmbeddedProperty;
    nzXl: number | EmbeddedProperty;
    nzXXl: number | EmbeddedProperty;
    [property: string]: any;
    /** temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289*/
    setClassMap(): void;
    generateClass(): object;
    constructor(nzUpdateHostClassService: NzUpdateHostClassService, elementRef: ElementRef, nzRowDirective: NzRowDirective, renderer: Renderer2);
    ngOnChanges(): void;
    ngAfterViewInit(): void;
    ngOnInit(): void;
    ngOnDestroy(): void;
}
