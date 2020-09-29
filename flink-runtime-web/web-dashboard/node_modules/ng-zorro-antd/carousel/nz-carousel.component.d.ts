import { AfterContentInit, AfterViewInit, ChangeDetectorRef, ElementRef, EventEmitter, NgZone, OnChanges, OnDestroy, QueryList, Renderer2, SimpleChanges, TemplateRef } from '@angular/core';
import { NzCarouselContentDirective } from './nz-carousel-content.directive';
export declare type NzCarouselEffects = 'fade' | 'scrollx';
export declare type SwipeDirection = 'swipeleft' | 'swiperight';
export declare class NzCarouselComponent implements AfterViewInit, AfterContentInit, OnDestroy, OnChanges {
    elementRef: ElementRef;
    private renderer;
    private cdr;
    private ngZone;
    slideContents: QueryList<NzCarouselContentDirective>;
    slickList: ElementRef;
    slickTrack: ElementRef;
    nzTransitionSpeed: number;
    nzDotRender: TemplateRef<{
        $implicit: number;
    }>;
    nzEffect: NzCarouselEffects;
    nzEnableSwipe: boolean;
    nzDots: boolean;
    nzVertical: boolean;
    nzAutoPlay: boolean;
    nzAutoPlaySpeed: number;
    readonly nzAfterChange: EventEmitter<number>;
    readonly nzBeforeChange: EventEmitter<{
        from: number;
        to: number;
    }>;
    activeIndex: number;
    transform: string;
    transitionAction: number | null;
    private el;
    private subs_;
    readonly nextIndex: number;
    readonly prevIndex: number;
    constructor(elementRef: ElementRef, renderer: Renderer2, cdr: ChangeDetectorRef, ngZone: NgZone);
    ngAfterContentInit(): void;
    ngAfterViewInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    ngOnDestroy(): void;
    setContentActive(index: number): void;
    private setTransition;
    next(): void;
    pre(): void;
    goTo(index: number): void;
    onKeyDown(e: KeyboardEvent): void;
    swipe(action?: SwipeDirection): void;
    swipeInProgress(e: any): void;
    clearTimeout(): void;
    /**
     * Make a carousel scroll to `this.nextIndex` after `this.nzAutoPlaySpeed` milliseconds.
     */
    private setUpNextScroll;
    private updateMode;
    private renderContent;
}
