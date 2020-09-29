import { ElementRef, OnInit, Renderer2 } from '@angular/core';
export declare class NzCarouselContentDirective implements OnInit {
    private elementRef;
    private renderer;
    el: HTMLElement;
    private _active;
    private _width;
    private _left;
    private _top;
    private _fadeMode;
    width: number;
    left: number | null;
    top: number | null;
    isActive: boolean;
    fadeMode: boolean;
    constructor(elementRef: ElementRef, renderer: Renderer2);
    ngOnInit(): void;
    private updateOpacity;
}
