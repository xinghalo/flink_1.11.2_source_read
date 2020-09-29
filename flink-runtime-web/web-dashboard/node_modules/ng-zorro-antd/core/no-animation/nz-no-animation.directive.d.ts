import { AfterViewInit, ElementRef, OnChanges, Renderer2 } from '@angular/core';
export declare class NzNoAnimationDirective implements OnChanges, AfterViewInit {
    private element;
    private renderer;
    private animationType;
    nzNoAnimation: boolean;
    constructor(element: ElementRef, renderer: Renderer2, animationType: string);
    ngOnChanges(): void;
    ngAfterViewInit(): void;
    private updateClass;
}
