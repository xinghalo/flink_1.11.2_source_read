import { ElementRef, OnChanges, OnInit, TemplateRef } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
export declare class NzDividerComponent implements OnChanges, OnInit {
    private elementRef;
    private nzUpdateHostClassService;
    nzText: string | TemplateRef<void>;
    nzType: 'horizontal' | 'vertical';
    nzOrientation: 'left' | 'right' | '';
    nzDashed: boolean;
    private setClass;
    constructor(elementRef: ElementRef, nzUpdateHostClassService: NzUpdateHostClassService);
    ngOnChanges(): void;
    ngOnInit(): void;
}
