import { ElementRef, OnInit } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzSizeLDSType } from '../core/types/size';
export declare class NzButtonGroupComponent implements OnInit {
    private nzUpdateHostClassService;
    private elementRef;
    nzSize: NzSizeLDSType;
    constructor(nzUpdateHostClassService: NzUpdateHostClassService, elementRef: ElementRef);
    private _size;
    private prefixCls;
    setClassMap(): void;
    ngOnInit(): void;
}
