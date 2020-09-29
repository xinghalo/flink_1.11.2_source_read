import { ComponentFactory, ComponentFactoryResolver, ElementRef, EventEmitter, OnInit, Renderer2, TemplateRef, ViewContainerRef } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzTooltipDirective } from '../tooltip/nz-tooltip.directive';
import { NzPopconfirmComponent } from './nz-popconfirm.component';
export declare class NzPopconfirmDirective extends NzTooltipDirective implements OnInit {
    noAnimation?: NzNoAnimationDirective | undefined;
    factory: ComponentFactory<NzPopconfirmComponent>;
    protected needProxyProperties: string[];
    nzOkText: string;
    nzOkType: string;
    nzCancelText: string;
    nzIcon: string | TemplateRef<void>;
    nzCondition: boolean;
    readonly nzOnCancel: EventEmitter<void>;
    readonly nzOnConfirm: EventEmitter<void>;
    constructor(elementRef: ElementRef, hostView: ViewContainerRef, resolver: ComponentFactoryResolver, renderer: Renderer2, tooltip: NzPopconfirmComponent, noAnimation?: NzNoAnimationDirective | undefined);
    ngOnInit(): void;
}
