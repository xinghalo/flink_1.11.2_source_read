import { AfterViewInit, ElementRef, OnDestroy, Renderer2 } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzColDirective } from '../grid/nz-col.directive';
import { NzRowDirective } from '../grid/nz-row.directive';
import { NzFormItemComponent } from './nz-form-item.component';
export declare class NzFormLabelComponent extends NzColDirective implements OnDestroy, AfterViewInit {
    nzFor: string;
    nzRequired: boolean;
    constructor(nzUpdateHostClassService: NzUpdateHostClassService, elementRef: ElementRef, nzFormItemComponent: NzFormItemComponent, nzRowDirective: NzRowDirective, renderer: Renderer2);
    ngOnDestroy(): void;
    ngAfterViewInit(): void;
}
