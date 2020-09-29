import { ElementRef, OnChanges, OnInit, Renderer2 } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
export declare class NzFormDirective implements OnInit, OnChanges {
    private elementRef;
    private renderer;
    private nzUpdateHostClassService;
    nzLayout: string;
    setClassMap(): void;
    constructor(elementRef: ElementRef, renderer: Renderer2, nzUpdateHostClassService: NzUpdateHostClassService);
    ngOnInit(): void;
    ngOnChanges(): void;
}
