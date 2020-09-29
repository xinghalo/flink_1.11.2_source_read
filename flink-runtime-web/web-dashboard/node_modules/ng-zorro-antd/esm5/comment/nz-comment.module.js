/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzCommentActionComponent, NzCommentActionHostDirective, NzCommentAvatarDirective, NzCommentContentDirective } from './nz-comment-cells';
import { NzCommentComponent } from './nz-comment.component';
/** @type {?} */
var NZ_COMMENT_CELLS = [
    NzCommentAvatarDirective,
    NzCommentContentDirective,
    NzCommentActionComponent,
    NzCommentActionHostDirective
];
var NzCommentModule = /** @class */ (function () {
    function NzCommentModule() {
    }
    NzCommentModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule, NzAddOnModule],
                    exports: tslib_1.__spread([NzCommentComponent], NZ_COMMENT_CELLS),
                    declarations: tslib_1.__spread([NzCommentComponent], NZ_COMMENT_CELLS)
                },] }
    ];
    return NzCommentModule;
}());
export { NzCommentModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY29tbWVudC5tb2R1bGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29tbWVudC9uei1jb21tZW50Lm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUMzRCxPQUFPLEVBQ0wsd0JBQXdCLEVBQ3hCLDRCQUE0QixFQUM1Qix3QkFBd0IsRUFDeEIseUJBQXlCLEVBQzFCLE1BQU0sb0JBQW9CLENBQUM7QUFDNUIsT0FBTyxFQUFFLGtCQUFrQixFQUFFLE1BQU0sd0JBQXdCLENBQUM7O0lBRXRELGdCQUFnQixHQUFHO0lBQ3ZCLHdCQUF3QjtJQUN4Qix5QkFBeUI7SUFDekIsd0JBQXdCO0lBQ3hCLDRCQUE0QjtDQUM3QjtBQUVEO0lBQUE7SUFLOEIsQ0FBQzs7Z0JBTDlCLFFBQVEsU0FBQztvQkFDUixPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsYUFBYSxDQUFDO29CQUN0QyxPQUFPLG9CQUFHLGtCQUFrQixHQUFLLGdCQUFnQixDQUFDO29CQUNsRCxZQUFZLG9CQUFHLGtCQUFrQixHQUFLLGdCQUFnQixDQUFDO2lCQUN4RDs7SUFDNkIsc0JBQUM7Q0FBQSxBQUwvQixJQUsrQjtTQUFsQixlQUFlIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOekFkZE9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9hZGRvbi9hZGRvbi5tb2R1bGUnO1xuaW1wb3J0IHtcbiAgTnpDb21tZW50QWN0aW9uQ29tcG9uZW50LFxuICBOekNvbW1lbnRBY3Rpb25Ib3N0RGlyZWN0aXZlLFxuICBOekNvbW1lbnRBdmF0YXJEaXJlY3RpdmUsXG4gIE56Q29tbWVudENvbnRlbnREaXJlY3RpdmVcbn0gZnJvbSAnLi9uei1jb21tZW50LWNlbGxzJztcbmltcG9ydCB7IE56Q29tbWVudENvbXBvbmVudCB9IGZyb20gJy4vbnotY29tbWVudC5jb21wb25lbnQnO1xuXG5jb25zdCBOWl9DT01NRU5UX0NFTExTID0gW1xuICBOekNvbW1lbnRBdmF0YXJEaXJlY3RpdmUsXG4gIE56Q29tbWVudENvbnRlbnREaXJlY3RpdmUsXG4gIE56Q29tbWVudEFjdGlvbkNvbXBvbmVudCxcbiAgTnpDb21tZW50QWN0aW9uSG9zdERpcmVjdGl2ZVxuXTtcblxuQE5nTW9kdWxlKHtcbiAgaW1wb3J0czogW0NvbW1vbk1vZHVsZSwgTnpBZGRPbk1vZHVsZV0sXG4gIGV4cG9ydHM6IFtOekNvbW1lbnRDb21wb25lbnQsIC4uLk5aX0NPTU1FTlRfQ0VMTFNdLFxuICBkZWNsYXJhdGlvbnM6IFtOekNvbW1lbnRDb21wb25lbnQsIC4uLk5aX0NPTU1FTlRfQ0VMTFNdXG59KVxuZXhwb3J0IGNsYXNzIE56Q29tbWVudE1vZHVsZSB7fVxuIl19